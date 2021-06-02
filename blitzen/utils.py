import zlib
import dill
import pickle
import socket
import datetime
from .logging import get_logger
from multiprocessing.connection import Client

def get_local_ip():
  return socket.gethostbyname(socket.gethostname())

def get_free_port():
  """
    Returns an OS determined available socket.
  """
  s = socket.socket(socket.AF_INET)
  s.bind(('localhost', 0))
  port = s.getsockname()[1]
  s.close()
  return port

class Promise:
  """
    Client side packet for waiting for incoming packages and syncing 
    subprocesses accross remote nodes.
  """

  def __init__(self, address):
    self.conn = None
    self.address = address

  def wait(self, authkey):
    """
      Connect to server at the promised address
      Then poll the underlying socket until data is returned by server.
      Once data has been received, closes the connection.

      This is done to permit client to poll sockets rather than the server.

      # Returns
      Data from the server endpoint specified by the underlying address.
    """
    if not self.conn:
      self.connect(authkey)

    while not self.conn.poll(): #Wait for data to be available
      pass

    data = self.conn.recv().unpack()
    self.conn.close()
    self.conn = None

    return data
  
  def connect(self, authkey, timeout=None):
    """
      Connects to server at the promised address.

      Returns the client side connection that can be used to communincate with 
      the server.
    """
    if not self.conn:
      
      now = datetime.datetime.now
      start_time = now()
      while self.conn is None and \
        (timeout is None or (now() - start_time).total_seconds() > timeout):
        self._connect(authkey)
    
    return self.conn
  
  def _connect(self, authkey):
    try:
      self.conn = Client(self.address, authkey=authkey)
      connected = True
    except ConnectionRefusedError:
      logger = get_logger()
      logger.warn('Promised connection was refused by server. Reattempting...')
      connected = False
    return connected

class Packet:
  """
    A basic Packet class for synchronization of data via Proxys.
    Proxies will return a Packet object. Use packet.unpack() to obtain 
    contained data.
  """

  def __init__(self,data):
    self.data = data
    self.times_compressed = 0
    self.serialize_method = None
  
  def unpack(self,):
    """
      Utility function that decompresses then returns the data stored in the 
      packet.
    """
    self.decompress()
    self.deserialize()
    return self.data
  
  def serialize(self, method=None):
    """
      Serializes `self.data` using the prescribed method. 
      This is useful for transferring complex data types across a network.

      method: One of ['pickle', 'dill']. If `None`, will attempt to pickle, 
        and resort to dill when necessary.
    """
    serialized = self.data
    if method is None:
      try:
        serialized = pickle.dumps(self.data)
        self.serialize_method = 'pickle'
      except Exception:
        serialized = dill.dumps(self.data) #Can throw dill error, should do so.
        self.serialize_method = 'dill'
    else:
      if method == 'pickle':
        serialized = pickle.dumps(self.data)
        self.serialize_method = 'pickle'
      elif method == 'dill':
        serialized = dill.dumps(self.data) #Can throw dill error, should do so.
        self.serialize_method = 'dill'
    
    self.data = serialized
    return self
  
  def deserialize(self,):
    """
      Deserializes self.data using the method that was used to serialize it by self.serialize().
    """
    deserialized = self.data
    if self.serialize_method:
      if self.serialize_method == 'pickle':
        deserialized = pickle.loads(self.data)
        self.serialize_method = None
      
      elif self.serialize_method == 'dill':
        deserialized = dill.loads(self.data)
        self.serialize_method = None
      
      else:
        msg = 'Cant Deserialize Packet. ' + \
          "Expected Serialization method to be one of [\'dill\', \'pickle\'], " + \
          f'but got {self.serialize_method}.'
        raise Exception(msg)
    
    self.data = deserialized
    return self 
    
  def compress(self, level=-1, iterations=1, threshold=0):
    """
      Compresses `self.data` to reduce payload across Pipes
      Useful when sending data accross a Proxy or Pipe to a remote manager.

      # Arguments
      level: ZLIB compress parameter, -1 to 9 that dictates compression vs time
        efficiency. 1 is lowest compression but fastest. 9 is greatest 
        compression. 0 means no compression, -1 means to intuit what level will \
        be the most efficient for speed and memory
      iterations: How many times to compress. If None, will continuue compression
        until further compression no longer saves memory.
      threshold: Maximum size in bytes for the payload before compression is 
        deemed necessary. If the serialized payload exceeds this much 
        data, then it will also be compressed. If `None`, then will serialize, 
        but not compress.
    """
    
    data = self.data
    if threshold is not None and len(data) > threshold: #If packet is sufficiently large, compress

      if iterations is None: #Compress until compression adds bytes
        compressed = self._compress(self.data, level)
        while len(compressed) < len(data):
          data = compressed
          compressed = self._compress(compressed, level) #adds 1 at end that must be offset
        self.times_compressed -= 1 #offsetting to omit final compression

      elif iterations >= 1:
        compressed = self.data
        for _ in range(iterations):
          compressed = self._compress(compressed, level)
        data = compressed
    
    self.data = data
    return self
  
  def _compress(self, data, level):
    compressed = zlib.compress(data, level=level)
    self.times_compressed += 1 #Should add after compress finished so try/catch can be managed elsewhere
    return compressed
  
  def decompress(self):
    """
      Identifies if deserialization or decompression is necessary. If so, 
      this function deserializes and/or decompresses the stored data.
    """
    data = self.data
    times_compressed = self.times_compressed
    for i in range(times_compressed):
      data = zlib.decompress(data)
      self.times_compressed -= 1
    
    self.data = data
    return self