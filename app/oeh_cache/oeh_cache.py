from dataclasses import dataclass, field

from app.oeh_elastic.helper_classes import Bucket, Collection, CollectionEncoder

import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@dataclass
class Cache:
    cache_dir: Path = Path.cwd() / Path("app") / Path("cache")
    filename: Path = field(init=False)
    collection_buckets: list[Bucket] = field(default_factory=list)  # list of collections and how often they  appear in resources
    fachportale: list[Collection] = field(default_factory=list)  # list of fachportale
    fachportale_with_children: dict[Collection, set[Collection]] = field(default_factory=dict)

    def __post_init__(self):
        self.filename = self.cache_dir / Path("cache.json")

    def serialize_fachportale_with_children(self):
        serialized = {}
        for key in self.fachportale_with_children:
            serialized[key.id] = [c.as_dict() for c in self.fachportale_with_children[key]]
        return serialized

    def save_cache_to_disk(self):
        logger.info("saving cache to disk...")
        self.make_file()

        cache = self.serialize_attributes()

        with self.filename.open("w") as f:
            json.dump(cache, f)

        logger.info("cache saved!")
        return True

    def serialize_attributes(self):
        collections_bucket = [b.as_dict() for b in self.collection_buckets]
        fachportale = [c.as_dict() for c in self.fachportale]
        fachportale_with_children = self.serialize_fachportale_with_children()

        to_cache = [collections_bucket, fachportale, fachportale_with_children]
        cache_keys = ["collection_buckets", "fachportale", "fachportale_with_children"]
        cache = {
            k: v for k, v in zip(cache_keys, to_cache)
        }
        return cache

    def make_file(self):
        self.cache_dir.mkdir(exist_ok=True)
        self.filename.touch()

    def deserialize_fachportale_with_children(self, data: dict):
        deserialized = {}
        for key in data:
            fachportal = next(f for f in self.fachportale if f.id == key)
            children = []
            for item in data[key]:
                children.append(Collection.from_json(item))
            deserialized[fachportal] = children
        return deserialized

    def load_cache_from_disk(self):
        with self.filename.open("r") as f:
            data = json.load(f)

        # TODO maybe it makes more sense to load and save the cache seperatly and raise errors when one of them is not available
        if collection_buckets := data["collection_buckets"]:
            self.collection_buckets = [Bucket.from_json(item) for item in collection_buckets]
        else:
            raise KeyError("Collection Buckets not found")
        self.fachportale = [Collection.from_json(item) for item in data["fachportale"]]
        self.fachportale_with_children = self.deserialize_fachportale_with_children(data["fachportale_with_children"])

    def load_cache(self):
        if self.filename.exists():
            self.load_cache_from_disk()
            logger.info("Cache loaded")
            return True
        else:
            logger.warning("Could not load cache from disk!")
            return False

    def empty_cache(self):
        logger.info("Emptying cache...")
        self.collection_buckets = []
        self.fachportale = []
        self.fachportale_with_children = {}

# if __name__ == "__main__":
#     c = Cache()
#     c.save_cache_to_disk()
