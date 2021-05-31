# Blitzen  

Blitzen is a python framework built on top of python's multiprocessing module intended to maximize processor utilization on individual computers and clusters.

# Installation  

```pip install blitzen```  

# Usage:  
Blitzen uses a `dispatcher` to provide concurrent processing features. 
Below are the dispatchers available and their use case.

## MulticoreDispatcher  
Multicore dispatcher is similar to `multiprocessing.Pool` in that you initialize the dispatcher and can pass it tasks to complete concurrently. 

The notable differences are:
1. Workers you create with a dispatcher will remain open until you shutdown the dispatcher. 
2. You can queue multiple tasks, and the dispatcher will complete all of them using the specified number of processes.  
3. Workers are Exception resistant, meaning if the worker subprocess crashes, the dispatcher will shut down the process and initialize a new one until the specified number of workers are actively monitor incoming tasks.

Example:
```Python
import time
import random
from blitzen import MulticoreDispatcher

def f1():
  delay = random.randint(3,5)
  time.sleep(delay)
  print('Finished after', delay, 'secs.')
  return delay

def f2(delay):
  time.sleep(delay)
  raise ValueError('Throwing error')


if __name__ == '__main__':
  dispatcher = MulticoreDispatcher(workers=4)
  task_id = dispatcher.run(f2, 4)
  task_ids = [
    dispatcher.run(f1)
    for _ in range(5)
  ]
```  
All tasks are either started or queued immediately upon running the `dispatcher.run()` call.

`dispatcher.run()` returns the dispatcher's task_id for the task you just passed it. This is used if you want to get specific task results from the dispatcher or tasks results in a specific order.

### Getting results from the dispatcher:
```Python
print('Fetching Results.')
results = dispatcher.get_results(task_ids)
print('Recevied results from dispatcher:', results)
```

The results are always returned in the order of `task_ids`. 

Eventually you will have to shutdown the dispatcher to close the underlying worker processes. You can do this by explicitly calling `dispatcher.shutdown()`; however, if you would like to get the results and perform the shutdown in one line you can call `dispatcher.join()`. This results the all results in the order they were passed in to the dispatcher, then calls `dispatcher.shutdown()`:

```Python
print('Fetching Results.')
results = dispatcher.join()
print('Recevied results from dispatcher:', results)
```

A full example is visible [here](https://github.com/BlakeERichey/blitzen/blob/main/examples/multicore/demo.py).

## DistributedDispatcher  
DistributedDispatcher lets you utilize a cluster for concurrent computing. It will handle packet synchronization between clients, servers, and drivers. It will also log all activity on the clients and servers.  

### Define some common functions  
The clients and servers need access to the same functions, so it is likely a good idea to make a common file that will be used to import functions that arent natively in python.  

```Python
#common.py
import time

def time_consuming_function(delay):
  time.sleep(delay)
  return delay
```

### Initialize your server  
```Python
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function

if __name__ == '__main__':
  ip = get_local_ip()
  backend = DistributedDispatcher(server_ip=ip)
  backend.spawn_server(duration=30)
```

### Initialize your clients  
```Python
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function

if __name__ == '__main__':
  ip = '192.168.1.2' #Server IP
  backend = DistributedDispatcher(server_ip=ip)
  backend.spawn_client(workers=6)
```  

### Run your driver code to be executed by the clients  
```Python
from blitzen.utils import get_local_ip
from blitzen.distributed import DistributedDispatcher
from common import time_consuming_function

if __name__ == '__main__':
  ip = '192.168.1.2' #Server IP
  backend = DistributedDispatcher(server_ip=ip)

  #With DistributedDispatcher you can specify an amount
  #of time each client has to finish their task
  #with the `timeout` keyword
  task_ids = [
    backend.run(time_consuming_function, i+5, timeout=7) 
    for i in range(5)
  ]

  print('Requesting results.')
  results = backend.join()
  print(results)
```

### Logging  
DistributedDispatchers provide some logging, using the python logging module, so you can monitor your cluster. By default the logger is configured to report all logging info of level `logging.INFO`. To some this can be excessive, so blitzen provides some functions to control the logger.  

You can get the logger using 
```logger = blitzen.logging.get_logger()```  
This logger is used by all blitzen submodules.  

The other blitzen.logging fuctions are:
* `set_logfile(filename)` lets you set a file to log out to.
* `set_loglevel(level)` lets you set the logging level using the same convention as python's `logging` module.
* `disable()` disabled the logger.
* `enable()` enables the logger.



A full example visible [here](https://github.com/BlakeERichey/blitzen/tree/main/examples/distributed).