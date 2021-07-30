import logging
from dataclasses import asdict, dataclass
from typing import List

from fastapi import FastAPI
from numpy import inf

from app.oeh_elastic import oeh
from app.oeh_elastic.helper_classes import Collection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()


@dataclass
class CollectionChildren:
    id: Collection
    children: List[Collection]


@app.get("/")
def read_root():
    logger.info("Hello there")
    return {"Hello": "World!"}


@app.get("/fachportale")
def get_collection_children():
    raw_collections = oeh.get_fachportale()
    parsed_collections = [c.as_dict() for c in raw_collections]
    return {"fachportale": parsed_collections}


# TODO add collections (fachportale) endpoint to return list of alle collections
@app.get("/fachportale/{id}/children", response_model=CollectionChildren)
def get_collection_children(id: str):
    raw_id = oeh.get_collection_info(id=id)
    parsed_id = raw_id.as_dict()
    raw_children: set[Collection] = oeh.get_collection_children(
        collection_id=id,
        doc_threshold=inf)
    parsed_children = [c.as_dict() for c in raw_children]
    return {"id": parsed_id, "children": parsed_children}
