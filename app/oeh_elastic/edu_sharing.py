from time import sleep

import requests
import logging

from app.oeh_elastic.constants import MAX_CONN_RETRIES
from app.oeh_elastic.helper_classes import Collection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class EduSharing:
    connection_retries: int = 0

    @classmethod
    def get_collections(cls) -> list[dict]:
        ES_COLLECTIONS_URL = "https://redaktion.openeduhub.net/edu-sharing/rest/collection/v1/collections/local/5e40e372-735c-4b17-bbf7-e827a5702b57/children/collections?scope=TYPE_EDITORIAL&skipCount=0&maxItems=1247483647&sortProperties=cm%3Acreated&sortAscending=true&"

        headers = {
            "Accept": "application/json"
        }

        params = {
            "scope": "TYPE_EDITORIAL",
            "skipCount": "0",
            "maxItems": "1247483647",
            "sortProperties": "cm%3Acreated",
            "sortAscending": "true"
        }

        logger.info(f"Collecting Collections from edu-sharing...")

        try:
            r_collections: list = requests.get(
                ES_COLLECTIONS_URL,
                headers=headers,
                params=params
            ).json().get("collections")
            cls.connection_retries = 0
            return r_collections

        except:
            if cls.connection_retries < MAX_CONN_RETRIES:
                cls.connection_retries += 1
                logger.error(
                    f"Connection error trying to reach edu-sharing repository, trying again in 30 seconds. Retries: {cls.connection_retries}")
                sleep(30)
                return EduSharing.get_collections()

    def parse_collections(self, raw_collections: list[dict]) -> set[Collection]:
        collections = set()
        for item in raw_collections:
            title = item.get("title", None)
            name = item.get("name", None)
            _type = item.get("type", None)
            id = item.get("ref", {}).get("id", None)
            collections.add(Collection(
                id=id,
                name=name,
                title=title,
                type=_type
            ))
        return collections


edu_sharing = EduSharing()