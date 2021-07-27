from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypedDict, Literal

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
class CollectionInfo:
    id: str
    path: list = field(default_factory=list)
    es_url: str = field(init=False)
    name: str = ""
    title: str = ""
    type: str = ""
    content_url: str = ""
    action: str = ""
    count_total_resources: int = 0

    def __post_init__(self):
        if self.type == 'ccm:map':
            self.es_url = ES_COLLECTION_URL.format(self.id)
        else:
            self.es_url = ES_NODE_URL.format(self.id, self.action)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, CollectionInfo):
            return self.id == o.id
        else:
            return False

    def __hash__(self) -> int:
        return hash((self.id,))

    def as_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "path": self.path,
            "count_total_resources": self.count_total_resources
        }


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
