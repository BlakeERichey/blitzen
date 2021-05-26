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

    task_ids = []
    for i in range(5):
        task_id = backend.run(time_consuming_function, i)
        task_ids.append(task_id)
    
    print('Waiting before getting results.')
    # time.sleep(25)
    results = backend.get_results(task_ids)
    print(results)