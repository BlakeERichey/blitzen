from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function, PORT

if __name__ == '__main__':
    ip = get_local_ip()
    backend = DistributedDispatcher(server_ip=ip, port=PORT)

    task_ids = [
      backend.run(time_consuming_function, i+5, timeout=7)
      for i in range(5)
    ]

    print('Requesting results.')
    results = backend.join()
    print(results)