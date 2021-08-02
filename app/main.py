import logging

from fastapi import FastAPI
from numpy import inf

from app.oeh_elastic.helper_classes import Collection, CollectionChildrenResponse, CollectionResponse
from app.oeh_elastic.oeh import OEHElastic

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()

oeh = OEHElastic()
oeh.load_cache(update=False)


@app.get("/")
def read_root():
    logger.info("Hello there")
    return {"Hello": "World!"}


@app.get("/fachportale")
def get_fachportale():
    raw_collections = oeh.get_fachportale()
    parsed_collections = [c.as_dict() for c in raw_collections]
    return {"fachportale": parsed_collections}


@app.get("/fachportale/{id}", response_model=CollectionResponse)
def get_fachportal_by_id(id: str):
    raw_id = oeh.get_collection_info(id=id)
    parsed_id = raw_id.as_dict()
    return {"id": parsed_id}


@app.get("/fachportale/{id}/children", response_model=CollectionChildrenResponse)
def get_collection_children(id: str):
    raw_id = oeh.get_collection_info(id=id)
    parsed_id = raw_id.as_dict()
    raw_children: set[Collection] = oeh.get_collection_children(
        collection_id=id,
        doc_threshold=inf)
    parsed_children = [c.as_dict() for c in raw_children]
    return {"id": parsed_id, "children": parsed_children}
