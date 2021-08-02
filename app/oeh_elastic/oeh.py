#!/usr/bin/env python3

import logging
import os
from collections import Counter, defaultdict
from time import sleep
from typing import Generator, Literal, Union

import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from numpy import inf

from app.main import oeh
from app.oeh_elastic.edu_sharing import EduSharing, edu_sharing
from app.oeh_elastic.constants import MAX_CONN_RETRIES, SOURCE_FIELDS
from app.oeh_elastic.elastic_query import AggQuery
from app.oeh_cache.oeh_cache import Cache
from app.oeh_elastic.helper_classes import Bucket, Collection, SearchedMaterialInfo

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class OEHElastic:
    es: Elasticsearch

    def __init__(self, hosts=None) -> None:
        if hosts is None:
            hosts = [os.getenv("ES_HOST", "localhost")]
        self.connection_retries = 0
        self.es = Elasticsearch(hosts=hosts)
        self.last_timestamp = "now-30d"  # get values for last 30 days by default
        # dict with collections as keys and a list of Searched Material Info as values
        self.searched_materials_by_collection: dict[str, SearchedMaterialInfo] = {}
        self.all_searched_materials: set[SearchedMaterialInfo] = set()
        self.cache = Cache()

        # TODO we might turn this on again later
        # self.get_oeh_search_analytics(
        #     timestamp=None, count=ANALYTICS_INITIAL_COUNT)

    def query_elastic(self, body, index, pretty: bool = True):
        try:
            r = self.es.search(body=body, index=index, pretty=pretty)
            self.connection_retries = 0
            return r
        except ConnectionError:
            if self.connection_retries < MAX_CONN_RETRIES:
                self.connection_retries += 1
                logger.error(
                    f"Connection error while trying to reach elastic instance, trying again in 30 seconds. Retries {self.connection_retries}")
                sleep(30)
                return self.query_elastic(body, index, pretty)

    def load_cache(self, update: bool = False):
        if update is False:
            if self.cache.load_cache():
                return True
            else:
                logger.warning("could not load cache...building...")
                self.build_cache()
                self.save_cache()
        elif update:
            logger.info("Updating cache")
            self.build_cache()
            self.save_cache()


    def build_cache(self):
        self.cache.empty_cache()
        logger.info("Building collecition bucket cache...")
        self.cache.collection_buckets = self.build_collection_buckets_cache()
        logger.info("Building fachportale cache...")
        self.cache.fachportale = self.build_fachportale_cache()
        logger.info("building fachportale with children cache...")
        self.cache.fachportale_with_children = self.build_fachportale_with_children_cache()

    def save_cache(self):
        self.cache.save_cache_to_disk()

    # TODO add save function
    def build_collection_buckets_cache(self) -> list[Bucket]:
        collection_aggregations_query = AggQuery(attribute="collections.nodeRef.id.keyword")
        collection_aggregations_cache = self.get_aggregations(collection_aggregations_query)
        return self.build_buckets_from_agg(collection_aggregations_cache)

    # TODO add save function
    def build_fachportale_cache(self) -> set[Collection]:
        return self.build_fachportale()

    def build_fachportale_with_children_cache(self) -> dict[Collection, list[Collection]]:
        fachportale_with_children = {}
        for item in self.cache.fachportale:
            children = self.get_collection_children(collection_id=item.id)
            fachportale_with_children[item] = children
        return fachportale_with_children

    def get_fachportale(self):
        if self.cache.fachportale:
            return self.cache.fachportale
        else:
            self.build_fachportale_cache()

    def get_collection_buckets(self):
        return self.cache.collection_buckets

    def get_collection_children(
            self,
            collection_id: str,
            doc_threshold: Union[float, int] = inf
    ) -> set[Collection]:
        """
        Returns a set of CollectionInfo class 
        if there is no material present in that collection (or less than the threshold value).

        :type doc_threshold: Union[float, int]
        :param collection_id: ID of the Fachportal you want to look information up from.
        :param doc_threshold: Threshold of documents to be at least in a collection
        """
        logger.info(f"getting collections with threshold of \"{doc_threshold}\" and key: \"{collection_id}\"")

        # TODO This might be useful in some other case, so maybe put it outside as class method
        def check_number_of_resources_in_collection(collection_id: str) -> bool:
            """
            Checks if there are resources (ccm:io) that have a given collection id in there path.
            We check if the total hits are lower than or equal the doc_threshold value, because that indicates
            how many documents are present in that collection.
            :param collection_id:
            :return:
            """
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "type": [
                                        "ccm:io"
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "should": [
                                        {
                                            "match": {
                                                "path": collection_id
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                "_source": SOURCE_FIELDS
            }
            r = self.query_elastic(body=body, index="workspace")
            total_hits = r.get("hits").get("total").get("value")

            if total_hits <= doc_threshold:
                return True
            else:
                return False

        def parse_raw_collection_children(raw_collection_children: dict) -> set[Collection]:
            collections = set()
            for item in raw_collection_children.get("hits", {}).get("hits", []):
                # TODO add this to a parse function
                id = item.get("_source").get("nodeRef").get("id")
                title = item.get("_source").get("properties").get("cm:title", "")
                path = item.get("_source").get("path", [])
                doc_count = oeh.get_statisic_counts(collection_id=id, attribute="nodeRef.id").get("hits").get("total").get("value", 0)

                if doc_count <= doc_threshold and check_number_of_resources_in_collection(id):
                    collections.add(Collection(
                        id=id,
                        title=title,
                        count_total_resources=doc_count,
                        path=path))
            return collections

        if self.cache.fachportale_with_children:
            logger.info("Returning result from cache")
            return self.cache.fachportale_with_children[Collection(collection_id)]
        else:
            logger.info("Querying elastic")
            raw_collection_children = self.get_collection_children_by_id(collection_id)
            collection_children: set[Collection] = parse_raw_collection_children(raw_collection_children)
            return collection_children

    def build_fachportale(self):
        raw_collections: list[dict] = edu_sharing.get_collections()
        fachportale = edu_sharing.parse_collections(raw_collections=raw_collections)
        for item in fachportale:
            count_total_resources: int = oeh.get_statisic_counts(collection_id=item.id, attribute="nodeRef.id").get("hits").get("total").get("value", 0)
            item.count_total_resources = count_total_resources
        return fachportale

    def get_base_condition(self, collection_id: str = None, additional_must: dict = None) -> dict:
        must_conditions = [
            {"terms": {"type": ['ccm:io']}},
            {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
            {"terms": {"properties.cm:edu_metadataset": ['mds_oeh']}},
            {"terms": {"nodeRef.storeRef.protocol": ['workspace']}},
        ]
        if additional_must:
            must_conditions.append(additional_must)

        if collection_id:
            must_conditions.append(
                {"bool": {
                    "should": [
                        {"match": {"collections.path": collection_id}},
                        {"match": {"collections.nodeRef.id": collection_id}},
                    ],
                    "minimum_should_match": 1
                }
                }
            )
        return {
            "bool": {
                "must": must_conditions
            }
        }

    def get_collection_by_missing_attribute(self, collection_id: str, attribute: str, size: int = 10000) -> dict:
        """
        Returns an es-query-result with collections that have a given missing attribute.
        If count is set to 0, only the total number will be returned.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ['ccm:map']}},
                        {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
                        {"bool": {
                            "should": [
                                {"match": {"path": collection_id}},
                                {"match": {"nodeRef.id": collection_id}}
                            ],
                            "minimum_should_match": 1
                        }
                        },
                    ],
                    "must_not": [{"wildcard": {attribute: "*"}}]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": size,
            "track_total_hits": True
        }
        # print(body)
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_collection_children_by_id(self, collection_id: str):
        """
        Returns a list of children of a given collection_id
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ["ccm:map"]}},
                        {"bool": {
                            "should": [
                                {"match": {"path": collection_id}},
                            ]
                        }
                        }
                    ]
                }
            },
            "size": 10000,
            "track_total_hits": "true",
            "_source": SOURCE_FIELDS
        }
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_collection_info(self, id: str) -> Collection:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ["ccm:map"]}},
                        {"bool": {
                            "should": [
                                {"match": {"nodeRef.id": id}},
                            ]
                        }
                        }
                    ]
                }
            },
            "size": 10000,
            "track_total_hits": "true",
            "_source": SOURCE_FIELDS
        }
        if collection := next(f for f in self.cache.fachportale if f.id == id):
            return collection
        else:
            r: dict = self.query_elastic(body=body, index="workspace", pretty=True)
            collection = self.parse_query_result(r.get("hits", {}).get("hits")[0])
            return collection

    def parse_query_result(self, result: dict) -> Collection:
        id = result.get("_source").get("nodeRef").get("id")
        title = result.get("_source").get("properties").get("cm:title", "")
        name = result.get("_source").get("properties").get("cm:name", "")
        path = result.get("_source").get("path", [])
        doc_count = self.get_statisic_counts(collection_id=id).get("hits").get("total").get("value", 0)

        return Collection(
            id=id,
            title=title,
            name=name,
            path=path,
            count_total_resources=doc_count
        )

    def get_material_by_missing_attribute(self, collection_id: str, attribute: str, size: int = 10000) -> dict:
        """
        Returns the es-query result for a given collection_id and the attribute.
        If count is set to 0, just the total number will be returned in the es-query-result.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.get_base_condition(collection_id),
                    ],
                    "must_not": [{"wildcard": {attribute: "*"}}]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": size,
            "track_total_hits": True
        }
        # pprint(body)
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_statisic_counts(self, collection_id: str,
                            attribute: str = "properties.ccm:commonlicense_key.keyword") -> dict:
        """
        Returns count of values for a given attribute (default: license) in a collection.
        Can also be used to get the total count of resources in a fachportal or collection.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.get_base_condition(collection_id),
                    ]
                }
            },
            "aggs": {
                "license": {
                    "terms": {
                        "field": attribute,
                    }
                }
            },
            "size": 0,
            "track_total_hits": True
        }
        # print(body)
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_material_by_condition(self, collection_id: str, condition: Literal["missing_license"] = None,
                                  count=10000) -> dict:
        """
        Returns count of values for a given attribute (default: license)
        """
        if condition == "missing_license":
            additional_condition = {
                "terms": {
                    "properties.ccm:commonlicense_key.keyword": ["NONE", "", "UNTERRICHTS_UND_LEHRMEDIEN"]
                }
            }
        else:
            additional_condition = None
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.get_base_condition(
                            collection_id, additional_condition),
                    ]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": count,
            "track_total_hits": True
        }
        # print(body)
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_oeh_search_analytics(self, timestamp: str = None, count: int = 10000):
        """
        Returns the oeh search analytics.
        """

        def filter_search_strings(unfiltered: list[dict]) -> Generator:
            for item in unfiltered:
                search_string = item.get(
                    "_source", {}).get("searchString", None)
                if search_string and search_string.strip() != "":
                    yield search_string
                else:
                    continue

        if not timestamp:
            gt_timestamp = self.last_timestamp
            logger.info(f"searching with a gt timestamp of: {gt_timestamp}")
        else:
            gt_timestamp = timestamp
            logger.info(f"searching with a given timestamp of: {gt_timestamp}")

        body = {
            "query": {
                "range": {
                    "timestamp": {
                        "gt": gt_timestamp,
                        "lt": "now"
                    }
                }
            },
            "size": count,
            "sort": [
                {
                    "timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }
        query = self.query_elastic(
            body=body, index="oeh-search-analytics", pretty=True)
        r: list[dict] = query.get("hits", {}).get("hits", [])

        # set last timestamp to last timestamp from response
        if len(r):
            self.last_timestamp = r[0].get("_source", {}).get("timestamp")

        filtered_search_strings = filter_search_strings(r)
        search_counter = Counter(list(filtered_search_strings))

        def check_timestamp(new, old):
            if new > old:
                return new
            else:
                return old

        def filter_for_terms_and_materials(res: list[dict]):
            """
            :param list[dict] res: result from elastic-search query
            """
            all_materials: set[SearchedMaterialInfo] = set()
            filtered_res = (item for item in res if item.get(
                "_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source", {}) for item in filtered_res):
                clicked_resource_id = item.get("clickedResult").get("id")
                timestamp: str = item.get("timestamp", "")

                clicked_resource = SearchedMaterialInfo(
                    _id=clicked_resource_id,
                    timestamp=timestamp
                )

                search_string: str = item.get("searchString", "")

                # we got to check the FPs for the given resource
                logger.info(
                    f"checking included fps for resource id: {clicked_resource}")

                # build the object
                if not clicked_resource in self.all_searched_materials:
                    logger.info(
                        f"{clicked_resource} not present, creating entry, getting info...")
                    result: SearchedMaterialInfo = self.get_resource_info(
                        clicked_resource._id, list(collections_ids_title.keys()))
                    result.timestamp = timestamp
                    result.search_strings.update([search_string])
                    self.all_searched_materials.add(result)
                else:
                    logger.info(f"{clicked_resource!r} present, updating...")
                    old = next(
                        e for e in self.all_searched_materials if e == clicked_resource)
                    # check for newest timestamp
                    new_timestamp = check_timestamp(timestamp, old.timestamp)

                    clicked_resource.title = old.title
                    clicked_resource.name = old.name
                    clicked_resource.fps = old.fps
                    clicked_resource.creator = old.creator
                    clicked_resource.search_strings.update([search_string])
                    clicked_resource.search_strings += old.search_strings
                    clicked_resource.clicks = old.clicks + 1
                    clicked_resource.timestamp = new_timestamp
                    self.all_searched_materials.remove(old)
                    self.all_searched_materials.add(clicked_resource)

            return True

        # we have to check if path contains one of the edu-sharing collections with an elastic query
        # get fpm collections
        collections = EduSharing.get_collections()
        collections_ids_title = {item.get("properties").get(
            "sys:node-uuid")[0]: item.get("title") for item in collections}
        materials_by_terms = filter_for_terms_and_materials(r)

        # assign material to fpm portals
        collections_by_material = defaultdict(list)
        for item in sorted(self.all_searched_materials, reverse=True):  # key is the material id
            if fps := item.fps:
                for fp in fps:
                    collections_by_material[fp].append(item)
            else:
                collections_by_material["none"].append(item)

        self.searched_materials_by_collection = collections_by_material

    def get_node_path(self, node_id) -> dict:
        """
        Queries elastic for a given node and returns the collection paths
        """

        body = {
            "query": {
                "match": {
                    "nodeRef.id": node_id
                }
            },
            "_source": [
                "properties.cclom:title",
                "properties.cm:name",
                "collections.path",
                "properties.ccm:replicationsource",
                "properties.cm:creator"
            ]
        }
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_resource_info(self, resource_id: str, collection_ids: list) -> SearchedMaterialInfo:
        """
        Gets info about a resource from elastic
        """
        try:
            hit: dict = self.get_node_path(resource_id).get(
                "hits", {}).get("hits", [])[0]
            paths = hit.get("_source").get(
                "collections", [{}])[0].get("path", [])
            name = hit.get("_source").get("properties", {}).get(
                "cm:name", None)  # internal name
            title = hit.get("_source").get("properties", {}).get(
                "cclom:title", None)  # readable title
            content_url = hit.get("_source").get("properties", {}).get(
                "ccm:wwwurl", None)  # Source page url
            crawler = hit.get("_source").get("properties", {}).get(
                "ccm:replicationsource", None)
            creator = hit.get("_source").get("properties", {}).get(
                "cm:creator", None)
            included_fps = {path for path in paths if path in collection_ids}
            return SearchedMaterialInfo(
                _id=resource_id,
                name=name,
                title=title,
                clicks=1,
                crawler=crawler,
                content_url=content_url,
                creator=creator,
                fps=included_fps
            )
        except Exception as e:
            logger.exception(e)
            return SearchedMaterialInfo()

    def get_aggregations(
            self,
            agg_query: AggQuery
    ) -> dict:
        """
        Returns the aggregations for a given attribute.
        """
        r: dict = self.query_elastic(body=agg_query.body, index=agg_query.index, pretty=True)
        return r

    def build_buckets_from_agg(self, agg: dict, include_other: bool = False) -> list[Bucket]:
        """
        Builds the buckets from an aggregation query.
        Return is a list of dicts with keys: key, doc_count
        """

        def build_buckets(buckets):
            return [Bucket(b["key"], b["doc_count"]) for b in buckets]

        my_agg = agg.get("aggregations", {}).get("my-agg", {})
        buckets: list[Bucket] = build_buckets(
            my_agg.get("buckets", []))

        if include_other:
            other_count: int = my_agg.get("sum_other_doc_count")
            other_bucket = Bucket("other_doc_count", other_count)
            buckets.append(other_bucket)
        return buckets

    def get_doc_count_from_missing_agg(self, agg: dict) -> Bucket:
        doc_count = agg.get("aggregations", {}).get("my-agg", {}).get("doc_count", None)
        bucket = Bucket("missing", doc_count)
        return bucket

    def build_df_from_buckets(self, buckets) -> pd.DataFrame:
        d = [b.as_dict() for b in buckets]
        df = pd.DataFrame(d)
        return df

    def sort_searched_materials(self) -> list[SearchedMaterialInfo]:
        """
        Sorts searched materials by last click.
        """
        searched_materials_all: set[SearchedMaterialInfo] = set()
        for key in self.searched_materials_by_collection:
            searched_materials_all.update(
                self.searched_materials_by_collection[key])
        sorted_search = sorted(
            searched_materials_all,
            key=lambda x: x.timestamp,
            reverse=True)
        return sorted_search


oeh = OEHElastic()
oeh.load_cache(update=False)

if __name__ == "__main__":
    print("\n\n\n\n")
    r = oeh.get_collection_info(id="4940d5da-9b21-4ec0-8824-d16e0409e629")
    r = oeh.get_collection_children(collection_id="4940d5da-9b21-4ec0-8824-d16e0409e629", doc_threshold=inf)
    r_parsed = [c.as_dict() for c in r]
    print(r)

