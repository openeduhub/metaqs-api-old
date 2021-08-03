from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypedDict, Literal, Optional, List

from .constants import (ES_COLLECTION_URL, ES_NODE_URL,
                        ES_PREVIEW_URL)


@dataclass
class LicenseInfo:
    licenses_dict: dict
    missing_licenses: set[str]


@dataclass
class Bucket:
    key: str
    doc_count: int

    def as_dict(self):
        return {
            "key": self.key,
            "doc_count": self.doc_count

        }

    @staticmethod
    def from_json(item: dict):
        return Bucket(key=item.get("key"), doc_count=item.get("doc_count"))

    def as_wc(self):
        return {
            "text": self.key,
            "value": self.doc_count
        }

    def __eq__(self, o) -> bool:
        if self.key == o:
            return True
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.key,))


@dataclass
class GeneralResource:
    """
    Attributes:

    - id: :class:`str` Id of the resource
    - path: :class:`list[str]`
    - es_url: :class:`str`
    - count_total_resources :class:`Optional[int]` Total count of documents inside the collection
    """
    id: str
    path: Optional[list[str]] = field(default_factory=list)
    es_url: str = field(init=False)
    name: Optional[str] = None
    title: Optional[str] = None
    type: Optional[str] = None
    content_url: Optional[str] = None
    action: Optional[str] = None
    creator: Optional[str] = None

    def __post_init__(self):
        if self.type == 'ccm:map':
            self.es_url = ES_COLLECTION_URL.format(self.id)
        else:
            self.es_url = ES_NODE_URL.format(self.id, self.action)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, GeneralResource):
            return self.id == o.id
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.id,))

    def __lt__(self, o: object):
        if isinstance(o, GeneralResource):
            return self.id < o.id
        else:
            raise NotImplemented

    @staticmethod
    def from_elastic_query_hits(item: dict):
        return GeneralResource(
            id=item.get("_source").get("nodeRef").get("id"),
            name=item.get("_source").get("properties").get("cm:name"),
            title=item.get("_source").get("properties").get("cclom:title"),
            type=item.get("_source").get("type")
        )


# TODO add more attribute documentation
@dataclass
class Collection(GeneralResource):
    resources_with_no_description: Optional[set] = None
    resources_with_no_licenses: Optional[set] = None
    count_total_resources: Optional[int] = 0
    licenses: Optional[dict] = None

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Collection):
            return self.id == o.id
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.id,))

    def __lt__(self, o: object):
        if isinstance(o, Collection):
            return self.id < o.id
        else:
            raise NotImplemented

    @classmethod
    def set_to_list(cls, set_items):
        if set_items:
            return [item for item in set_items]
        else:
            return []

    def as_dict(self):
        return {
            "id": self.id,
            "path": self.path,
            "es_url": self.es_url,
            "name": self.name,
            "title": self.title,
            "type": self.type,
            "content_url": self.content_url,
            "action": self.action,
            "count_total_resources": self.count_total_resources,
            "licenses": self.licenses,
            "resources_with_no_licenses": self.set_to_list(self.resources_with_no_licenses),
            "resources_with_no_description": self.set_to_list(self.resources_with_no_description)
        }

    @classmethod
    def list_to_set(cls, list_items):
        if list_items:
            return {item for item in list_items}
        else:
            return set()

    @classmethod
    def from_json(cls, item: dict):
        return Collection(
            id=item.get("id"),
            path=item.get("path", []),
            name=item.get("name", None),
            title=item.get("title", None),
            type=item.get("type", "ccm:map"),
            content_url=item.get("content_url", None),
            action=item.get("action", None),
            count_total_resources=item.get("count_total_resources", 0),
            licenses=item.get("licenses", None),
            resources_with_no_licenses=cls.list_to_set(item.get("resources_with_no_licenses", None)),
            resources_with_no_description=cls.list_to_set(item.get("resources_with_no_description", None))
        )


@dataclass
class Material(GeneralResource):
    search_strings: Counter = field(default_factory=Counter)
    clicks: int = 0
    crawler: str = ""
    timestamp: str = ""  # timestamp of last access on material (utc)
    fps: set = field(default_factory=set)

    def __repr__(self) -> str:
        return self.id

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Material):
            return self.id == o.id
        else:
            return False

    def __lt__(self, o: object):
        if isinstance(o, Material):
            return self.timestamp < o.timestamp
        else:
            raise NotImplemented

    def __hash__(self) -> int:
        return hash((self.id,))

    def as_dict(self):
        search_term_count = "\"{}\"({})"  # term, count
        return {
            "id": self.id,
            "search_strings": ", ".join([search_term_count.format(term, count) for term, count in self.search_strings.items()]),
            "clicks": self.clicks,
            "name": self.name,
            "title": self.title,
            "crawler": self.crawler,
            "creator": self.creator,
            "timestamp": self.timestamp,
            "local_timestamp": (datetime.fromisoformat(self.timestamp[:-1]) + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "thumbnail_url": ES_PREVIEW_URL.format(self.id)
        }


@dataclass
class CollectionChildrenResponse:
    id: Collection
    children: List[Collection]


@dataclass
class CollectionResponse:
    id: Collection
