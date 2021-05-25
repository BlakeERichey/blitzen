from socket import socket
from common import time_consuming_function, PORT
from distributed import DistributedDispatcher
from utils import get_local_ip

import logging

logger = logging.getLogger()
logger.setLevel(30)

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)
    backend.spawn_server(20)