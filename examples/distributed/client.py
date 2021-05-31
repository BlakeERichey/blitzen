from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function, PORT

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)
    backend.spawn_client(6)