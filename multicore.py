import datetime
from threading import Thread
from multiprocessing import Pipe, Process
from utils import Packet
from base import BaseDispatcher

class MulticoreDispatcher(BaseDispatcher):

  def __init__(self, n_processes=1, timeout=None):
    """
      Initializes a MulticoreDispatcher with a configurable number of cores
      
      #Arguments
      n_processes: Int. Max number of subprocesses to spawn.
      timeout: Max time in seconds to permit a Process to execute any given task.
        If None, the processes will continue to run until self.shutdown() is called.
    """
    

    self._close_thread   = False       #If True, monitor thread will close
    self.tasks           = {}          #task_id: Task
    self.active          = set()       #task_ids for active tasks
    self.queued          = set()       #task_ids that are waiting to be run
    self.timeout         = timeout     #max time for process
    self.n_processes     = n_processes #max number of processes to spawn at one time
    self.current_task_id = 1           #Manages task ids, tasks start from 1
    

    self.processes = {} #process_id: {'connections', 'process', 'running'}
    for p_id in range(self.n_processes):
      parent_conn, sub_conn = Pipe(duplex=True)
      p = Process(
        target=MulticoreDispatcher._spawn_subprocess, 
        args=(sub_conn,),
      )
      p.start()
      self.processes[p_id] = {
        'connections': (parent_conn, sub_conn),
        'process': p,
        'running': False
      }
    
    self.monitor_thread = Thread(
      target=MulticoreDispatcher._monitor,
      args=(self,)
    )
    self.monitor_thread.daemon = True
    self.monitor_thread.start()

  @staticmethod
  def _monitor(self):
    """
      Asynchronously monitors the dispatcher and performs the following:
      1. Assigns idled processes any available queued tasks
      2. Check each active tasks to ensure timeouts have not been reached
      3. Replaces crashed subprocesses with new subprocesses
      4. Receives task results from subprocesses
    """
    while not self._close_thread:
      
      ####### Check each active task for timeout or subprocess crash #######
      for task_id in list(self.active):
        #timeout
        task = self.tasks[task_id]
        timeout = task['timeout']
        dt = (datetime.datetime.now() - task['start_time']).total_seconds()
        timeout_reached = dt > timeout if timeout is not None else False
        
        #subprocess crashed
        p_id = task['p_id']
        pfield = self.processes[p_id]
        process = pfield['process']
        process_crashed = process.exitcode is not None

        if timeout_reached or process_crashed:
          #Kill attached process
          process.terminate()

          #Create new subprocess
          parent_conn, sub_conn = pfield['connections']
          new_process = Process(
            target=MulticoreDispatcher._spawn_subprocess, 
            args=(sub_conn,),
          )
          new_process.start()

          #Attach connections to new subprocess
          pfield.update({
            'connections': (parent_conn, sub_conn),
            'process': new_process,
            'running': False
          })

          #Mark task as completed
          task.update({
            'p_id': None,
            'running': False,
            'completed': True,
            'terminated_early': True
          })
          self.active.remove(task_id)


      ####### Check for completed tasks #######
      for p_id in self.processes.keys():
        pfield = self.processes[p_id]
        parent_conn = pfield['connections'][0]
        has_data = parent_conn.poll()
        if has_data:
          #Get response to task from subprocess
          packet = parent_conn.recv()
          response = packet.unpack()

          task_id = response['task_id']
          result  = response['result']

          task = self.tasks[task_id]
          task.update({
            'p_id': None,
            'result': result,
            'running': False,
            'completed': True,
          })
          self.active.remove(task_id)
          pfield['running'] = False


      ######## Get idle processes #######
      idle_processes = []
      for i in self.processes.keys():
        if not self.processes[i]['running']:
          idle_processes.append(i)

      ####### Assign task to any idle process #######
      p_index = len(idle_processes)-1
      while p_index >= 0:
        if len(self.queued):
          task_id = self.queued.pop()
          self.active.add(task_id)
          task = self.tasks[task_id]
          task['p_id'] = idle_processes[p_index]
          task['running'] = True
          task['start_time'] = datetime.datetime.now()

          packet = Packet({
            'task_id': task_id,
            'func': task['func'],
            'args': task['args'],
            'kwargs': task['kwargs'],
          })
          packet.compress(iterations=0)

          pfield = self.processes[idle_processes[p_index]]
          parent_conn = pfield['connections'][0]
          parent_conn.send(packet)

        idle_processes.pop()
        p_index -= 1
  
  @staticmethod
  def _spawn_subprocess(conn):
    """
      Spawns a subprocess that listens to a connection and performs the 
      tasks that are passed through the connection.
    """
    while True:
      #Get task
      packet = conn.recv()
      task = packet.unpack()
      func = task['func']
      args = task['args']
      kwargs = task['kwargs']

      #Perform task
      result = func(*args, **kwargs) #Create a perform task function for abstration???

      #Send reponse
      packet = Packet({
        'task_id': task['task_id'],
        'result': result
      })
      conn.send(packet)

  def _new_task(self,):
    #Task Schema
    task_id = self.current_task_id
    self.current_task_id += 1
    task = {
      'p_id':             None, 
      'start_time':       None, 
      'running':          False, 
      'result:':          None,
      'timeout':          self.timeout,
      'completed':        False,
      'terminated_early': False,
    }
    return task_id, task
  
  def shutdown(self,):
    """
      Shutdown monitor thread, terminates spawned subprocesses, and releases 
      memory allocation resources utilized by the dispatcher. 
    """
    self._close_thread = True
    self.monitor_thread.join()
    ######## Close processes, Free memory #########

    for pfield in self.processes.values():
      process = pfield['process']
      process.terminate()

    del self.tasks

  def run(self, func, *args, **kwargs):
    """
      Places a task into the dispatchers queued tasks for subprocess completion.

       # Arguements
      func: a function
      args: value based arguements for function `func`
      kwargs: keyword based arguements for `func`

      # Returns 
      The associated task id needed to recover the results.
    """
    task_id, task = self._new_task()
    task.update({
      'func': func,
      'args': args,
      'kwargs': kwargs,
    })
    self.queued.add(task_id)
    self.tasks[task_id] = task

    return task_id
  
  def clear_task(self, task_id):
    """
      Removes all traces of a task being present on the server.
      Removes the task_id from all task queues and opens memory for additional 
      tasks.
    """

    for collection in [self.queued, self.active]:
      if task_id in collection:
        collection.remove(task_id)
    
    self.tasks.pop(task_id, None)
  
  def get_results(self, task_ids=[], values_only=True, clear=True):
    """
      Gets the results to tasks associated with the passed in task_ids. 
      Hangs current thread until all the tasks are complete.

      task_ids: task_ids as generated by self.run(). These are used by the 
        server to identify which task to return the results for.
      values_only: if False returns dictionary that includes the task ids with its 
        results. Otherwise, returns the values computed in order of the 
        requested task ids.
      clear: If True, removes tasks from dispatcher memory after returning results.
    """
    waiting = True #Continue waiting for tasks to finish
    while waiting:
      
      waiting = False
      #Check that all tasks are completed
      for task_id in task_ids:
        task = self.tasks[task_id]

        if not task['completed']:
          waiting = True

    results = []
    for task_id in task_ids:
      task = self.tasks[task_id]
      
      result = task.get('result')
      results.append(result)
      if clear:
        self.clear_task(task_id)
    
    if values_only:
      retval = results
    else:
      retval = dict(zip(task_ids, results))

    return retval
