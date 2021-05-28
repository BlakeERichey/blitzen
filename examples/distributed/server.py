import time
import logging
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function, PORT

logger = logging.getLogger()
logger.setLevel(30)

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)
    backend.spawn_server()