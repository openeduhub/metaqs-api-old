import uvicorn
from fastapi.logger import logger
import logging
import yaml

from app.main import app

# setup logger
with open('app/config.yml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
    logging.config.dictConfig(config)


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    logger.info("Hello there")
    uvicorn.run(
        "debug_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        debug=True,
        reload_dirs=["./app/oeh_cache", "./app/oeh_elastic"]
    )
