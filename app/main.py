import uvicorn
from fastapi import FastAPI
from numpy import inf

from dataclasses import asdict

from app.oeh_elastic import oeh
from app.oeh_elastic.helper_classes import CollectionInfo


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World!"}


# TODO add collections endpoint to return list of alle collections
@app.get("/collections/{id}/collections")
def read_collection_id(id: str):
    r: set[CollectionInfo] = oeh.get_collections(
        collection_id=id,
        doc_threshold=inf)
    r_parsed = [asdict(c) for c in r]
    return {"id": id, "children": r_parsed}
