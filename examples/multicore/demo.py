from multicore import MulticoreDispatcher
import time
import random

def f1():
  delay = random.randint(3,5)
  time.sleep(delay)
  print('Finished after', delay, 'secs.')
  return delay

def f2(delay):
  time.sleep(delay)
  raise ValueError('Throwing error')


if __name__ == '__main__':
  try:
    backend = MulticoreDispatcher(4)
    task_id = backend.run(f2, 4)
    task_ids = [task_id]
    for i in range(5):
      task_id = backend.run(f1)
      task_ids.append(task_id)

    print('Fetching Results to tasks:', task_ids)
    results = backend.get_results(task_ids, values_only=True)
    print('Recevied results from backend:', results)
  finally:
    backend.shutdown()