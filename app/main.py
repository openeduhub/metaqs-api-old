import uvicorn
from fastapi import FastAPI
from numpy import inf

from dataclasses import asdict, dataclass
from typing import List

from app.oeh_elastic import oeh
from app.oeh_elastic.helper_classes import CollectionInfo


app = FastAPI()

@dataclass
class CollectionChildren:
    id: str
    children: List[CollectionInfo]

@app.get("/")
def read_root():
    return {"Hello": "World!"}


# TODO add collections (fachportale) endpoint to return list of alle collections
@app.get("/collections/{id}/collections", response_model=CollectionChildren)
# @app.get("/collections/{id}/collections")
def read_collection_id(id: str):
    id_info = oeh.get_collection_info(id=id)
    r: set[CollectionInfo] = oeh.get_collection_children(
        collection_id=id,
        doc_threshold=inf)
    r_parsed = [asdict(c) for c in r]
    return {"id": id, "children": r_parsed}
