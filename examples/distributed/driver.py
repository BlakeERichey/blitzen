import time
import logging
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function, PORT

logger = logging.getLogger()
logger.setLevel(0)

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)

    task_ids = [
      backend.run(time_consuming_function, i+5, timeout=10)
      for i in range(5)
    ]

    print('Requesting results.')
    results = backend.join()
    print(results)