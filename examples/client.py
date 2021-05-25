from common import PORT, time_consuming_function
from distributed import DistributedDispatcher
import logging
from utils import get_local_ip

logger = logging.getLogger()
logger.setLevel(0)

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)
    backend.spawn_client(4)