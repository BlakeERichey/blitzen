import datetime
from logging import log
from threading import Thread
from multiprocessing import Pipe, Lock
from multiprocessing.connection import Listener
from multiprocessing.managers import BaseManager
from .logging import get_logger
from .utils import Packet, Promise, get_free_port

class ParallelManager(BaseManager):  
  def __init__(self, *args, timeout=None, task_limit=None, revive_limit=0, **kwargs):
    """
      Initlizes a parallel manager for distributed task management.

      # Arguments
      address: (string server_ip, int port). 
        Port to open on the server and its IP address for remote connections.
      authkey: authorization key to connect to remote manager.
      timeout: Default time in seconds to permit for a task. 
        If a task takes longer, the server ceases to await a response.
      task_limit: Int. Max number of tasks for server to remember. 
        This monitors the total number of active, completed, and queued tasks.
      revive_limit: Int. Max number of times a client can reach a timeout before 
        it is disconnected from the server.
    """
    super().__init__(*args, **kwargs)

    self.timeout    = timeout
    self.task_limit = task_limit

    self.current_task_id   = 1
    self.current_client_id = 1 #Client id as used by the server to assign tasks
    self.fetch_results_thread_id  = 1
    self.revive_limit = revive_limit #Limit to number of times a timeout can occur on client
    self.clients = {
      #client_id: {
      #  'listener':   mp.Listener,
      #  'connection': mp.connection.Connection,
      #  'busy':   False,
      #  'alive':  True,
      #  'times_dead': 0 #Used to gauge continued revival of this client
      #}
    }
    self.fetch_results = {
      # 'id': {
      #   'thread': Thread,
      #   'listener': mp.Listener
      # }
    }
    self.queued_tasks    = set() #task_ids
    self.active_tasks    = set() #task_ids
    self.completed_tasks = set() #task_ids
    self.tasks = {} #{task_id: Task}... 
    # Task SCHEMA:
    # task = {
    #   'task_id':          task_id,
    #   'func':             func,
    #   'args':             args,
    #   'kwargs':           kwargs,
    #   'start_time':       None, 
    #   'running_on':       None, #client_id 
    #   'completed':        False,
    #   'result':           None,
    #   'timeout':          timeout or self.timeout,
    #   'terminated_early': False
    # }
    
    #Exposed methods to remote clients and drivers (via authkey)
    self.register('submit',      callable=self.submit)
    self.register('monitor',     callable=self.monitor)
    self.register('get_results', callable=self.get_results)
  
  @staticmethod
  def _monitor_clients(self,):
    """
      Runs asynchonously to monitor server. 
      Performs the following functions:
        1. Assigns idled clients any available queued tasks
        2. Check each active tasks to ensure timeouts have not been reached
        3. Marks clients with timeouts as 'dead'; listens for a revival packet
        4. Receives task results from clients
        5. Close results-fetching threads.
    """
    while not self._close_thread:
      ######## Critical Code (Async access to variables prohibited) ########
      self.critical_lock.acquire()

      ######## Check for queued tasks and idle clients ######## 
      if len(self.queued_tasks):
        idle_clients = self._get_idle_clients()

        if len(idle_clients):
          num_tasks_to_assign = min(len(idle_clients), len(self.queued_tasks))
          for i in range(num_tasks_to_assign):
            client_id = idle_clients[i]
            self._send_queued_task(client_id)

      ######## Check for timeouts and mark clients as 'dead' ########
      if len(self.active_tasks):
        tasks_to_kill = set()

        for task_id in self.active_tasks:
          task = self.tasks[task_id]
          start_time   = task.get('start_time')
          max_duration = task.get('timeout')
          duration = (datetime.datetime.now() - start_time).total_seconds()

          if max_duration and duration > max_duration:
            tasks_to_kill.add(task_id)
        
        self._kill_tasks(tasks_to_kill)
      
      ######## Receive task results and check for revival packet ########
      #Use list because popping from self.clients in event of connection error
      client_ids = list(self.clients.keys())
      for client_id in client_ids:
        client = self.clients[client_id]
        server_conn = client['connection']
        
        if server_conn:
          task_done = server_conn.poll()
          if task_done:
            try:
              packet = server_conn.recv()
            except Exception as e:
              msg = 'Exception occurred while reading from client. ' + \
                'Terminating connection.'
              self._kill_client(client_id)
              get_logger().warning(msg)
              continue

            data = packet.unpack()

            result  = data['result']
            task_id = data['task_id']

            if task_id in self.active_tasks:
              self._complete_task(task_id, result)
            elif task_id in self.completed_tasks: #Task was terminated early
              #Do not update results on an already marked `completed` task
              client['alive'] = True
      
      ######## Close results-fetching threads ########
      thread_id_keys = list(self.fetch_results.keys()) #Because popping from self.fetch_results
      for thread_id in thread_id_keys:
        tfield = self.fetch_results[thread_id]
        thread = tfield['thread']
        #   thread has started          #and thread is finished
        if thread._started.is_set() and not thread.is_alive(): #Thread has finished
          thread.join()
        
          self.fetch_results.pop(thread_id) #Remove request

      ######## Permit other threads temporary access to variables ########
      self.critical_lock.release()
    
    get_logger().info('Closing Monitor Thread.')

  def _start_sub_thread(self,):
    self._close_thread = False  #Used is shutdown to terminate monitor thread
    self.critical_lock = Lock() #Used for synchronizing critical points in async code
    self.monitor_thread = Thread(
      target=ParallelManager._monitor_clients, 
      args=(self,)
    )
    self.monitor_thread.daemon = True
    self.monitor_thread.start()


  def start(self, initializer=None, initargs=()):
    self.register('cleanup',           callable=self.cleanup)
    self.register('_start_sub_thread', callable=self._start_sub_thread)

    super().start(initializer, initargs)
    self._start_sub_thread()

  def cleanup(self,):
    """
      Shuts down subthreads and releases memory allocations of tasks
    """
    get_logger().info('Shutting down monitor threads.')
    self._close_thread = True
    self.monitor_thread.join()

    get_logger().info('Releasing memory allocations.')
    del self.tasks
    client_ids = list(self.clients.keys())
    for client_id in client_ids:
      client = self.clients[client_id]
      listener = client['listener']
      server_conn = client['connection']
      server_conn.close()
      listener.close()
      del self.clients[client_id]
  
  def _get_idle_clients(self,):
    idle_clients = []
    for client_id, client in self.clients.items():
      if client['alive'] and not client['busy'] and client['connection']:
        idle_clients.append(client_id)
    
    return idle_clients

  def _get_new_task_id(self):
    """
      Returns a new task_id.
      Used for internal monitoring of tasks.
    """
    task_id = self.current_task_id
    self.current_task_id += 1
    return task_id
  
  def _get_new_fetch_results_thread_id(self):
    """
      Returns a new task_id.
      Used for internal monitoring of tasks.
    """
    thread_id = self.fetch_results_thread_id
    self.fetch_results_thread_id += 1
    return thread_id
  
  def _get_new_client_id(self,):
    """
      Returns a new task_id.
      Used for internal managing of client connections.
    """
    client_id = self.current_client_id
    self.current_client_id += 1
    return client_id

  def submit(self, func, *args, timeout=None, **kwargs):
    """
      Interface for 'clients' to submit a problem and its dependencies to the 
      problem hoster, the 'server'. The server hosts this problem and loads it 
      into a self regulated task queue. 

      #Returns server's identifying task id if submission was successful. 
      #Otherwise, None. 

      # Arguments:
      func:    an exectuable function.
      args:    all arguments to be passed into func.
      kwargs:  all keyword arguments to be passed into func
      timeout: the max time in seconds to permit the function to be in 
        operation. If None, the default for the manager, as created
        by __init__(), will be used.
    """
    ######## Critical Code (Async access to variables prohibited) ########
    self.critical_lock.acquire()

    data = None
    if self.task_limit is None or len(self.tasks) < self.task_limit:
      task_id = str(self._get_new_task_id())
      self.queued_tasks.add(task_id)

      task = {
        'task_id':          task_id,
        'func':             func,
        'args':             args,
        'kwargs':           kwargs,
        'start_time':       None, 
        'running_on':       None, #client_id 
        'completed':        False,
        'result':           None,
        'timeout':          timeout or self.timeout,
        'terminated_early': False
      }
      self.tasks[task_id] = task

      data = task_id
      get_logger().info(f'Task received. Queued tasks: {len(self.queued_tasks)}.')
    
    self.critical_lock.release()
    return Packet(data)

  def monitor(self,):
    """
      Interface for client to monitor the server for active tasks
      Registers the client on the server and will permit assigning tasks to the 
      client
      
      Returns connection. The server will send packets to the client through 
      this connection when tasks are waiting in the servers task queue.
    """
    ######## Critical Code (Async access to variables prohibited) ########
    self.critical_lock.acquire()

    ip = self._address[0]
    port = get_free_port()
    address = (ip, port)
    listener = Listener(address, authkey=self._authkey)
    
    client_id = self._get_new_client_id()
    self.clients[client_id] = {
       'listener':   listener,
       'connection': None,
       'busy':       False,
       'alive':      True,
       'times_dead': 0
    }
    
    #Currently letting garbage collector manager joining this thread.
    thread = Thread(
      target=ParallelManager._connect_monitor_client,
      args=(self, client_id, listener)
    )
    thread.daemon = True
    thread.start()

    self.critical_lock.release()
    return Packet(Promise(address))
  
  @staticmethod
  def _connect_monitor_client(self, client_id, listener):
    server_conn = listener.accept()
    ip, port = listener.last_accepted
    get_logger().info(f'Client {client_id} connected at {ip}:{port}')
    
    ######## Critical Code (Async access to variables prohibited) ########
    self.critical_lock.acquire()
    
    self.clients[client_id]['connection'] = server_conn

    self.critical_lock.release()
  
  def _send_queued_task(self, client_id):
    """
      Sends queued task to client
      
      Finds a queued tasks and sends it to Client
    """
    client = self.clients[client_id]
    server_conn = self.clients[client_id]['connection']
    
    try:
      task_id = self.queued_tasks.pop() #if no task available, throws KeyError
      self.active_tasks.add(task_id)
      task = self.tasks[task_id]
      
      msg = f'Sending task {task_id} to client {client_id}.'
      get_logger().info(msg)

      needed_fields = {
        'task_id': task.get('task_id'),
        'func':    task.get('func'),
        'args':    task.get('args'),
        'kwargs':  task.get('kwargs'),
      }
      packet = Packet(needed_fields)
      server_conn.send(packet)

      msg = f'Task {task_id} sent. ' + \
        f'Queued tasks: {len(self.queued_tasks)}'
      get_logger().info(msg)

      #Update Client Status
      client['busy'] = True
      
      #Update Task Status
      modInfo = {
        'start_time': datetime.datetime.now(), 
        'running_on': client_id,
      }
      task.update(modInfo)
    except KeyError:
      pass
    except Exception as e:
      self.queued_tasks.add(task_id)
      msg = f'Error sending task {task_id} to client.\n' + \
        f'Terminating client {client_id} connection.'
      msg += f'\nException: {e}'
      self._kill_client(client_id)
      get_logger().warning(msg)

  
  def _kill_tasks(self, task_ids):
    """
      Terminates active tasks and closes manager to listening for a 
      response for those specific tasks. Called in event a timeout is reached.
      This is called to ensure tasks do not exceed their maximum duration.
    """
    for task_id in task_ids:
      task = self.tasks.get(task_id)
      if task and task['running_on']:
        client_id = task['running_on']
        #Update Client
        self._kill_client(client_id)

        #Update Task
        task['running_on'] = None
        task['completed'] = datetime.datetime.now()
        task['terminated_early'] = True

        get_logger().warning(f'Task {task_id} timed out on client {client_id}.')

        try:
          self.active_tasks.remove(task_id)
          self.completed_tasks.add(task_id)
        except Exception as e:
          get_logger().warning(f'Error occured killing tasks: {e}')
  
  def _kill_client(self, client_id):
    try:
      client = self.clients[client_id]
      client['alive'] = False
      client['busy']  = False
      client['times_dead'] += 1

      #Disconnect client
      if client['times_dead'] > self.revive_limit:
        listener = client['listener']
        server_conn = client['connection']
        server_conn.close()
        listener.close()
        del self.clients[client_id]
        get_logger().warning(f'Client {client_id} has been disconnected.')

    except KeyError: #Another thread has already killed process
      pass
  
  def _complete_task(self, task_id, result):
    task = self.tasks[task_id]
    client_id = task['running_on']

    #Update Client
    client = self.clients[client_id]
    client['busy']  = False
  

    #Update Task
    task['result']     = result
    task['running_on'] = None
    task['completed']  = datetime.datetime.now()
    get_logger().info(f'Results received for task {task_id}.')

    try:
      self.active_tasks.remove(task_id)
      self.completed_tasks.add(task_id)
    except Exception as e:
      get_logger().warning(f'Error occured completing task {task_id}: {e}')
  
  def _clear_task(self, task_id):
    """
      Removes all traces of a task being present on the server.
      Removes the task_id from all task queues and opens memory for additional 
      tasks.
    """

    for collection in [self.queued_tasks, self.active_tasks, self.completed_tasks]:
      if task_id in collection:
        collection.remove(task_id)
    
    self.tasks.pop(task_id, None)

  def get_results(self, task_ids=None, values_only=True, clear=True):
    """
      Interface for driver to request completed tasks' results

      task_ids: list of task_ids as generated by self.submit(). These are used by the 
        server to identify which task to return the results for. 
        If `None`, waits for and returns results for all tasks.
      values_only: Remove task_id before returning values. Returned answers are in 
        the order of the 'task_ids' parameter.
      clear: If True, removes task from server memory after returning results.
    """
    ######## Critical Code (Async access to variables prohibited) ########
    self.critical_lock.acquire()

    thread_id = self._get_new_fetch_results_thread_id()
    ip = self._address[0]
    port = get_free_port()
    address = (ip, port)
    listener = Listener(address, authkey=self._authkey)

    thread = Thread(
      target=ParallelManager._get_results_thread,
      args=(self, listener, task_ids, values_only, clear)
    )
    self.fetch_results[thread_id] = {
      'thread': thread,
      'listener': listener,
    }
    thread.start()

    self.critical_lock.release()

    promise = Promise(address)
    return Packet(promise)

  @staticmethod
  def _get_results_thread(self, listener, task_ids, values_only, clear):
    server_conn = listener.accept() #Accept client's connection. Send to client through this connection

    if task_ids is None:
      task_ids = list(self.tasks.keys())

    task_set = set(task_ids)
    while task_set.difference(self.completed_tasks):
      pass
    
    ######## Critical Code (Async access to variables prohibited) ########
    self.critical_lock.acquire()
    
    results = []
    for task_id in task_ids:
      task = self.tasks[task_id]
      
      result = task.get('result')
      results.append(result)
      if clear:
        self._clear_task(task_id)
    
    self.critical_lock.release()

    if values_only:
      data = results
    else:
      data = dict(zip(task_ids, results))

    packet = Packet(data)
    try:
      server_conn.send(packet)
    except:
      msg = 'Connection Error. Unable to fulfill fetch-results promise.'
      get_logger().critical(msg)
    server_conn.close()
    listener.close()