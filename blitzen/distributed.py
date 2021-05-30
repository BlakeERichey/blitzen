import logging
import datetime
from .utils import Packet, get_local_ip
from .base import BaseDispatcher
from .multicore import MulticoreDispatcher
from .managers import ParallelManager

class DistributedDispatcher(BaseDispatcher):

  def __init__(self, server_ip='127.0.0.1', port=50000, authkey=b'authkey', 
              task_limit=None, timeout=None, revive_limit=0):
    """
      Initializes a Distributed & Multicore Backend Remote Manager.
      # Arguments
      server_ip: String. IP address for Remote Server. Client machines must use 
        and be able to see this machine.
      port: Int. Port number to open for clients to interface with the manager.
      authkey: Byte string. Used to authenticate access to the manager.
      task_limit: Int. Max number of tasks for server to remember. 
        This monitors the total number of active, completed, and queued tasks.
      timeout: Default time in seconds to permit on ther server for a task. 
        If a task takes longer, the server ceases to await a response.
      revive_limit: Int. Max number of times a client can reach a timeout before 
        it is disconnected from the server.
    """

    self.port          = port
    self.authkey       = authkey
    self.server_ip     = server_ip
    self.manager_creds = (server_ip, port, authkey)
  
    # Start a shared manager server and access its queues
    self.manager = ParallelManager(
      address=(server_ip, port), 
      authkey=authkey, 
      timeout=timeout,
      task_limit=task_limit,
      revive_limit=revive_limit
    )

  def spawn_server(self, duration=None):
    """
      Initializes a server on the active thread for the alloted duration

      duration: time in seconds to leave server running. If `None`, 
      server will run and hang the current thread indefinitely.
    """
    manager = self.manager
    manager.start()
    
    start_time = datetime.datetime.now()

    # server = manager.get_server()
    ip = get_local_ip()
    print(f'Server started. Local IP: {ip}. Port {self.port}.')
    
    time_running = (datetime.datetime.now() - start_time).total_seconds()
    while duration is None or time_running <= duration:
      time_running = (datetime.datetime.now() - start_time).total_seconds()
    
    self.shutdown()

  def spawn_client(self, n_processes=1):
    """
      Uses the active thread to connect to the remote server.
      Sets the client to monitor the connected server for tasks. When tasks are 
      available, client will request the necessary functions and data to 
      complete, and then submit the computed result to the server.
      # Arguments
      n_processes: Int. How many processes to spawn on the client.
    """

    if n_processes > 1:
      dispatcher = MulticoreDispatcher(n_processes=n_processes)
      task_ids = []
      for i in range(n_processes):
        task_id = dispatcher.run(
          type(self)._spawn_client_wrapper, 
          *self.manager_creds
        )
        task_ids.append(task_id)
      dispatcher.get_results(task_ids) #results not needed
    else:
      type(self)._spawn_client_wrapper(*self.manager_creds)

  @staticmethod
  def _spawn_client_wrapper(server_ip, port, authkey):
    """
      Wrapper for multiprocessing backend to spawn clients in subprocesses.
    """
    manager = ParallelManager(address=(server_ip, port), authkey=authkey)
    manager.connect()
    
    logging.info(f'Connected. {manager.address}')
    promise = manager.monitor().unpack() #Register client with server.
    print('Received packet')
    conn = promise.connect(authkey)
    while True:
      logging.debug('Checking for tasks')
      
      while not conn.poll(): #Wait for data to be available
        pass
      
      packet = conn.recv()
      task = packet.unpack()
      logging.debug('Task Received', task)
      
      #Unpack info and compute result
      if task is not None:
        task_id   = task['task_id']
        func      = task['func']
        args      = task['args']
        kwargs    = task['kwargs']
        result    = func(*args, **kwargs)
        
        response = Packet({
            'task_id': task_id,
            'result': result
        })
        conn.send(response)
  
  def shutdown(self,):
    """
      Closes monitor threads of manager and associated subprocesses
    """
    logging.debug('Shutting down server.')
    try:
      self.manager.cleanup()
      self.manager.shutdown()
    except Exception as e:
      print(e)
      # pass

  def run(self, func, *args, timeout=None, **kwargs):
    """
      Places a task into the servers queued tasks for client completion.
      # Returns the associated task id to recover the results.
      # Arguments
      func: function to be run via clients. Needs to be a function visible to 
        the requesting machine, the server, and the clients.
      args: arguments for `func`.
      kwargs: keyword arguments for `func`.
      timeout: the max time in seconds to permit the function to be in 
        operation. If None, the default for the manager, as created
        by __init__(), will be used.
    """
    manager = self.manager
    manager.connect()
    packet = manager.submit(func, *args, timeout=timeout, **kwargs)
    task_id = packet.unpack()
    return task_id
  
  def get_results(self, task_ids=[], values_only=True, clear=True):
    """
      Gets the results to tasks associated with the passed in task_ids. 
      Hangs current thread until all the tasks are complete.

      task_ids: task_ids as generated by self.run(). These are used by the 
        server to identify which task to return the results for.
      values_only: if False returns dictionary that includes the task ids with its 
        results. Otherwise, returns the values computed in order of the 
        requested task ids.
      clear: If True, removes tasks from server memory after returning results.
    """
    manager = self.manager
    manager.connect()
    promise = manager.get_results(
        task_ids, 
        values_only=values_only, 
        clear=clear).unpack()
    results = promise.wait(self.authkey)
    return results