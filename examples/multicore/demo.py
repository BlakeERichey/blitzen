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
  dispatcher = MulticoreDispatcher(4)
  task_id = dispatcher.run(f2, 4)
  task_ids = [
    dispatcher.run(f1)
    for _ in range(5)
  ]
  
  print('Fetching Results.')
  results = dispatcher.join()
  print('Recevied results from dispatcher:', results)