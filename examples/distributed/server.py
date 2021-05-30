import time
import logging
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function, PORT

logger = logging.getLogger()
logger.setLevel(0)

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT, revive_limit=2)
    backend.spawn_server()