import uvicorn
from fastapi.logger import logger
import logging

from app.main import app, oeh


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    uvicorn.run(
        "debug_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        debug=True,
        reload_dirs=["./app/oeh_cache", "./app/oeh_elastic"]
    )
