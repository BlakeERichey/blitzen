import sys
from multiprocessing.connection import Listener, Client

def server():
    listener = Listener(authkey=b'secret password', family='AF_INET')
    print('Listening on', listener.address)
    conn = listener.accept()
    print('connection accepted from', listener.last_accepted)
    conn.send('hello')
    conn.close()
    listener.close()

if __name__ == '__main__':
    server()