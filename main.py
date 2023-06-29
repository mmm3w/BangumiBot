import asyncio
import logging

from bangumi import BangumiBackgroundTask, app
from bangumi.database import redisDB
from bangumi.downloader import downloader
from bangumi.util import init_folders, setup_env, setup_logger

logger = logging.getLogger(__name__)


# @app.on_event("startup")
# def startup():
#     setup_env()
#     setup_logger()
#     init_folders()

#     redisDB.connect()
#     downloader.connect()

#     BangumiBackgroundTask().start()
redisDB.connect()

redisDB.update_last_checked_time()
print(redisDB.get_last_checked_time())

print(redisDB.is_seeding('test'))
redisDB.set_seeding('test')
print(redisDB.is_seeding('test'))

redisDB.init()
