import os
from dotenv import load_dotenv
from numpy import inf

load_dotenv()

fpm_icons = {
    "Biologie": "./assets/_svg/Biologie.svg",
    "Chemie": "./assets/_svg/Chemie.svg",
    "Darstellendes Spiel": "./assets/_svg/Darstellendes_Spiel.svg",
    "Deutsch als Zweitsprache": "./assets/_svg/DAZ.svg",
    "Deutsch": "./assets/_svg/Deutsch.svg",
    "Englisch": "./assets/_svg/Englisch.svg",
    "Geschichte": "./assets/_svg/Geschichte.svg",
    "Informatik": "./assets/_svg/Informatik.svg",
    "Kunst": "./assets/_svg/Kunst.svg",
    "Mathematik":  "./assets/_svg/Mathematik.svg",
    "Medienbildung": "./assets/_svg/Medienbildung.svg",
    "Nachhaltigkeit": "./assets/_svg/Nachhaltigkeit.svg",
    "Physik": "./assets/_svg/Physik.svg",
    "Politische Bidlung": "./assets/_svg/Politik.svg",
    "Religion": "./assets/_svg/Religion.svg",
    "Spanisch": "./assets/_svg/Spanisch.svg",
    "Sport": "./assets/_svg/Sport.svg",
    "Türkisch": "./assets/_svg/Tuerkisch.svg"
}

# CONSTANTS
ES_COLLECTION_URL = "https://redaktion.openeduhub.net/edu-sharing/components/collections?id={}"
ES_NODE_URL = "https://redaktion.openeduhub.net/edu-sharing/components/render/{}?action={}"
ES_PREVIEW_URL = "https://redaktion.openeduhub.net/edu-sharing/preview?maxWidth=200&maxHeight=200&crop=true&storeProtocol=workspace&storeId=SpacesStore&nodeId={}"


def set_conn_retries():
    MAX_CONN_RETRIES = os.getenv("MAX_CONN_RETRIES", inf)
    if MAX_CONN_RETRIES == "inf":
        return inf
    else:
        if type(eval(MAX_CONN_RETRIES)) == int:
            return eval(MAX_CONN_RETRIES)
        else:
            raise TypeError(
                f"MAX_CONN_RETRIES: {eval(MAX_CONN_RETRIES)} is not an integer")


MAX_CONN_RETRIES = set_conn_retries()
SOURCE_FIELDS = [
    "nodeRef",
    "type",
    "properties.cclom:title",  # title of io objects,
    "properties.ccm:wwwurl",
    "properties.cm:name",
    "properties.cm:title",  # title of collections and objects
    "path"
]
ANALYTICS_INITIAL_COUNT = os.getenv("ANALYTICS_INITIAL_COUNT", 10000)