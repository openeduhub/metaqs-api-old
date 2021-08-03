#!/usr/bin/env python3

import logging
import os
from time import sleep
from typing import Literal, Optional, Union

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

from app.oeh_elastic.edu_sharing import edu_sharing
from app.oeh_elastic.constants import MAX_CONN_RETRIES, SOURCE_FIELDS
from app.oeh_cache.oeh_cache import Cache
from app.oeh_elastic.helper_classes import Bucket, Collection, Material, LicenseInfo, GeneralResource

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
        self.cache = Cache()

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
        logger.info("Building fachportale cache...")
        self.cache.fachportale = self.build_fachportale()
        logger.info("building fachportale with children cache...")
        self.cache.fachportale_with_children = self.build_fachportale_with_children_cache()

    def save_cache(self):
        self.cache.save_cache_to_disk()

    def build_fachportale_with_children_cache(self) -> dict[Collection, set[Collection]]:
        """
        For each Fachportal all children get queried.
        :return: dict[Collection, set[Collection]]
        """
        fachportale_with_children = {}
        # TODO for testing
        for item in list(self.cache.fachportale)[:1]:
            children = self.get_collection_children(collection_id=item.id)
            fachportale_with_children[item] = children
        return fachportale_with_children

    def get_fachportale(self) -> set[Collection]:
        if self.cache.fachportale:
            return self.cache.fachportale
        else:
            self.build_fachportale()

    def get_collection_children(self, collection_id: str) -> set[Collection]:
        """
        Returns a set of CollectionInfo class 
        if there is no material present in that collection (or less than the threshold value).

        :param collection_id: ID of the Fachportal you want to look information up from.
        :return: set[Collection]
        """
        logger.info(f"getting children of collection with key: \"{collection_id}\"")

        if self.cache.fachportale_with_children:
            logger.info("Returning result from cache")
            return self.cache.fachportale_with_children[Collection(collection_id)]
        else:
            logger.info("Querying elastic")
            raw_collection_children = self.get_collection_children_by_id(collection_id)
            collection_children: set[Collection] = self.parse_raw_collection_children(raw_collection_children)
            return collection_children

    def parse_raw_collection_children(self, raw_collection_children: dict) -> set[Collection]:
        """
        Parses the children of a collection.

        :param raw_collection_children:
        :return: set[Collection]
        """
        collections = set()
        for item in raw_collection_children.get("hits", {}).get("hits", []):
            parsed_collection = self.parse_raw_collection(elastic_response=item)
            collections.add(parsed_collection)
        return collections

    def parse_raw_collection(self, elastic_response: dict) -> Collection:
        """
        Parses a response from elastic and build a Collection out of it.

        :param elastic_response: Response from elastic query.
        :return: Collection
        """
        id = elastic_response.get("_source").get("nodeRef").get("id")
        title = elastic_response.get("_source").get("properties").get("cm:title", "")
        path = elastic_response.get("_source").get("path", [])
        doc_count = self.get_statistic_counts(collection_id=id, attribute="nodeRef.id").get("hits").get("total").get(
            "value", 0)
        license_info = self.get_licenses_for_collection(id=id)

        return Collection(
            id=id,
            title=title,
            count_total_resources=doc_count,
            path=path,
            licenses=license_info.licenses_dict,
            resources_with_no_licenses=license_info.missing_licenses
        )

    def build_fachportale(self) -> set[Collection]:
        """
        Builds a set of Fachportale available in wlo.
        :return: set[Collection]
        """
        logger.info("Building fachportale...")
        raw_collections: list[dict] = edu_sharing.get_collections()
        fachportale = edu_sharing.parse_collections(raw_collections=raw_collections)
        for item in fachportale:
            count_total_resources: int = self.count_total_resources_of_collection(item)
            item.count_total_resources = count_total_resources

            licenses_info = self.get_licenses_for_collection(id=item.id)
            item.licenses = licenses_info.licenses_dict
            item.resources_with_no_licenses = licenses_info.missing_licenses

            item.resources_with_no_description = self.get_resources_with_no_description()
        return fachportale

    def get_resources_with_no_description(self) -> set[str]:
        resources_with_no_description = set()
        raw_no_description_by_condition = self.get_material_by_condition(collection_id="bd8be6d5-0fbe-4534-a4b3-773154ba6abc", condition="missing_description")

        raw_no_description_by_property = self.get_material_by_missing_attribute_in_collection(collection_id="bd8be6d5-0fbe-4534-a4b3-773154ba6abc",
                                                            attribute="properties.cclom:general_description")

        no_description_results = (raw_no_description_by_condition, raw_no_description_by_property)
        for result in no_description_results:
            for item in result.get("hits").get("hits"):
                resource = self.parse_query_result(result_item=item)
                resources_with_no_description.add(resource)

        # FIXME for now we are just returning the ids, because it makes the json serialization easier
        resources_with_no_description_just_str = {item.id for item in resources_with_no_description}
        return resources_with_no_description_just_str

    def count_total_resources_of_collection(self, item: Collection) -> int:
        return self.get_statistic_counts(collection_id=item.id, attribute="nodeRef.id").get("hits").get("total").get(
            "value", 0)

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
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_collection_children_by_id(self, collection_id: str) -> dict:
        """
        Returns a list of children for a given collection_id.

        :param collection_id: str
        :return: dict Result of elastic query
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

    def parse_query_result(self, result_item: dict) -> Union[Collection, GeneralResource]:
        id = result_item.get("_source").get("nodeRef").get("id")
        title = result_item.get("_source").get("properties").get("cm:title", None)
        name = result_item.get("_source").get("properties").get("cm:name", None)
        path = result_item.get("_source").get("path", [])
        _type = result_item.get("_source").get("type", None)

        if _type == "ccm:map":
            doc_count = self.get_statistic_counts(collection_id=id).get("hits").get("total").get("value", 0)
            return Collection(
                id=id,
                type=_type,
                title=title,
                name=name,
                path=path,
                count_total_resources=doc_count
            )
        elif _type == "ccm:io":
            return GeneralResource(
                id=id,
                type=_type,
                title=title,
                name=name,
                path=path
            )

    def get_material_by_missing_attribute_in_collection(self, collection_id: str, attribute: str,
                                                        size: int = 10000) -> dict:
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
        return self.query_elastic(body=body, index="workspace", pretty=True)

    # TODO This could also be a generic query to find license info for every collection
    def get_licenses_for_collection(self, id: str) -> LicenseInfo:
        missing_licenses, sorted_licenses = self.parse_licenses_from_elastic(id)

        return LicenseInfo(licenses_dict=sorted_licenses, missing_licenses=missing_licenses)

    def parse_licenses_from_elastic(self, id: str) -> tuple[set, dict]:
        license_agg: dict = self.get_statistic_counts(
            collection_id=id,
            aggregation_name="license",
            attribute="properties.ccm:commonlicense_key.keyword"
        )
        license_buckets = self.build_buckets_from_agg(license_agg, agg_name="license")

        # get documents where license property is missing
        raw_missing_licenses_by_property = self.get_material_by_missing_attribute_in_collection(collection_id=id,
                                                                                                attribute="properties.ccm:commonlicense_key")
        count_missing_licenses_by_property = raw_missing_licenses_by_property.get("hits").get("total").get("value", 0)
        missing_licenses_by_property = {GeneralResource.from_elastic_query_hits(item).id for item in
                                        raw_missing_licenses_by_property.get("hits").get("hits")}

        # get documents where licenses are missing by value, e.g. "None" and "" values
        raw_missing_licenses_by_value = self.get_material_by_condition(collection_id=id, condition="missing_license")
        count_missing_licenses_by_value = raw_missing_licenses_by_value.get("hits").get("total").get("value", 0)
        missing_licenses_by_value = {GeneralResource.from_elastic_query_hits(item).id for item in
                                     raw_missing_licenses_by_value.get("hits").get("hits")}

        # add them up
        count_missing_licenses = count_missing_licenses_by_value + count_missing_licenses_by_property

        # build a license dictionary out of it
        sorted_licenses = self.sort_licenses(license_buckets, count_missing_licenses=count_missing_licenses)
        missing_licenses = missing_licenses_by_property.union(missing_licenses_by_value)
        return missing_licenses, sorted_licenses

    def sort_licenses(self, licenses: list[Bucket], count_missing_licenses: int) -> dict[str, int]:
        oer_cols = ["CC_0", "CC_BY", "CC_BY_SA", "PDM"]
        cc_but_not_oer = ["CC_BY_NC", "CC_BY_NC_ND",
                          "CC_BY_NC_SA", "CC_BY_SA_NC", "CC_BY_ND"]
        copyright_cols = ["COPYRIGHT_FREE", "COPYRIGHT_LICENSE", "CUSTOM"]
        missing_cols = ["", "NONE", "UNTERRICHTS_UND_LEHRMEDIEN"]

        licenses_sorted = {
            "oer": 0,
            "cc": 0,
            "copyright": 0,
            "missing": 0
        }

        for l in licenses:
            if l.key in oer_cols:
                licenses_sorted["oer"] += l.doc_count
            elif l.key in cc_but_not_oer:
                licenses_sorted["cc"] += l.doc_count
            elif l.key in copyright_cols:
                licenses_sorted["copyright"] += l.doc_count
            elif l.key in missing_cols:
                licenses_sorted["missing"] += l.doc_count
            else:
                raise KeyError(f'Could not find \"{l.key}\" in columns.')

        licenses_sorted["missing"] += count_missing_licenses
        return licenses_sorted

    def get_statistic_counts(
            self,
            collection_id: str,
            attribute: str = "properties.ccm:commonlicense_key.keyword",
            aggregation_name: str = "license"
    ) -> dict:
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
                aggregation_name: {
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

    def get_material_by_condition(self, collection_id: str, condition: Literal["missing_license", "missing_description"] = None,
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
        elif condition == "missing_description":
            additional_condition = {
                "terms": {
                    "properties.cclom:general_description.keyword": [
                        "",
                        " "
                    ]
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

    # TODO consolidate with get base condition function
    def get_node_path(self, node_id) -> dict:
        """
        Queries elastic for a given node_id
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

    def get_resource_info(self, resource_id: str) -> Optional[Material]:
        """
        Gets info about a resource from elastic.
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

            return Material(
                id=resource_id,
                name=name,
                title=title,
                crawler=crawler,
                content_url=content_url,
                creator=creator
            )
        except Exception as e:
            logger.exception(e)
            return None

    def build_buckets_from_agg(
            self,
            agg: dict,
            agg_name: str = "my-agg",
            include_other: bool = False
    ) -> list[Bucket]:
        """
        Builds the buckets from an aggregation query.
        Return is a list of dicts with keys: key, doc_count
        """

        def build_buckets(buckets):
            return [Bucket(b["key"], b["doc_count"]) for b in buckets]

        my_agg = agg.get("aggregations", {}).get(agg_name, {})
        buckets: list[Bucket] = build_buckets(
            my_agg.get("buckets", []))

        if include_other:
            other_count: int = my_agg.get("sum_other_doc_count")
            other_bucket = Bucket("other_doc_count", other_count)
            buckets.append(other_bucket)
        return buckets


if __name__ == "__main__":
    oeh = OEHElastic()
    oeh.load_cache(update=False)
    print("\n")