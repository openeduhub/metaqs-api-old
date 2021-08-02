from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypedDict, Literal, Optional, List
from json import JSONEncoder

from .constants import (ES_COLLECTION_URL, ES_NODE_URL,
                        ES_PREVIEW_URL)


class Licenses(TypedDict):
    oer: int
    cc: int
    copyright: int
    missing: int


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


# TODO add more attribute documentation
@dataclass
class Collection:
    """
    Attributes:

    - id: :class:`str` Id of the collection
    - path: :class:`list[str]`
    - es_url: :class:`str`
    - count_total_resources :class:`Optional[int]` Total count of documents inside the collection
    """
    id: str  # id of collection
    path: Optional[list[str]] = field(default_factory=list)
    es_url: str = field(init=False)
    name: Optional[str] = None
    title: Optional[str] = None
    type: Optional[str] = "ccm:map"
    content_url: Optional[str] = None
    action: Optional[str] = None
    count_total_resources: Optional[int] = 0

    def __post_init__(self):
        if self.type == 'ccm:map':
            self.es_url = ES_COLLECTION_URL.format(self.id)
        else:
            self.es_url = ES_NODE_URL.format(self.id, self.action)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Collection):
            return self.id == o.id
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.id,))

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
            "count_total_resources": self.count_total_resources
        }

    @staticmethod
    def from_json(item: dict):
        return Collection(
            id=item.get("id"),
            path=item.get("path", []),
            name=item.get("name", None),
            title=item.get("title", None),
            type=item.get("type", "ccm:map"),
            content_url=item.get("content_url", None),
            action=item.get("action", None),
            count_total_resources=item.get("count_total_resources", 0)
        )


class CollectionEncoder(JSONEncoder):
    def default(self, o: Collection):
        return o.as_dict()


@dataclass
class SearchedMaterialInfo:
    _id: str = ""
    search_strings: Counter = field(default_factory=Counter)
    clicks: int = 0
    name: str = ""
    title: str = ""
    content_url: str = ""
    crawler: str = ""
    creator: str = ""
    timestamp: str = ""  # timestamp of last access on material (utc)
    fps: set = field(default_factory=set)

    def __repr__(self) -> str:
        return self._id

    def __eq__(self, o: object) -> bool:
        if isinstance(o, SearchedMaterialInfo):
            return self._id == o._id
        else:
            return False

    def __lt__(self, o: object):
        return self.timestamp < o.timestamp

    def __hash__(self) -> int:
        return hash((self._id,))

    def as_dict(self):
        search_term_count = "\"{}\"({})"  # term, count
        return {
            "id": self._id,
            "search_strings": ", ".join([search_term_count.format(term, count) for term, count in self.search_strings.items()]),
            "clicks": self.clicks,
            "name": self.name,
            "title": self.title,
            "crawler": self.crawler,
            "creator": self.creator,
            "timestamp": self.timestamp,
            "local_timestamp": (datetime.fromisoformat(self.timestamp[:-1]) + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "thumbnail_url": ES_PREVIEW_URL.format(self._id)
        }


@dataclass
class QueryParams:
    attribute: str = None  # attribute to query
    collection_id: str = None  # only search attribute where collection id is nodeRef.id or in respective path
    index: str = None  # index to search in
    size: int = None  # number of elements to return from query
    agg_type: Literal["terms", "missing"] = None
    additional_must: dict = None


@dataclass
class CollectionChildrenResponse:
    id: Collection
    children: List[Collection]


@dataclass
class CollectionResponse:
    id: Collection
