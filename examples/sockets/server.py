import time
import random
import socket
from multiprocessing.connection import Listener, Client
from threading import Thread

def get_local_ip():
  return socket.gethostbyname(socket.gethostname())

def get_free_port():
    s = socket.socket(socket.AF_INET)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def socket_specific_task(port,):
    """
        Perform a task asynchonously after receiving a connection on the 
        specified port
    """
    address = (get_local_ip(), port)
    listener = Listener(address, authkey=b'secret password')
    print('Listening on', listener.address)
    conn = listener.accept()
    print('connection accepted from', listener.last_accepted)

    delay = random.randint(1,3)
    time.sleep(delay)

    conn.send(str(delay))
    
    conn.close()
    listener.close()

def server():
    address = (get_local_ip(), 50000) #Endpoint located on port 50000
    listener = Listener(address, authkey=b'secret password')
    print('Listening on', listener.address)
    conn = listener.accept()
    print('connection accepted from', listener.last_accepted)

    port = get_free_port() ##### Port not guarenteed to remain open after this line #####

    #Asynchonously perform task and respond to connections on the free port
    thread = Thread(
        target=socket_specific_task,
        args=(port,)
    )
    thread.daemon = True
    thread.start()
    
    conn.send(str(port)) #Send back port to expect results to come from
    
    conn.close()
    listener.close()
    thread.join()

if __name__ == '__main__':
    server()