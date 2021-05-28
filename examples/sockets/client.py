import sys
from multiprocessing.connection import Listener, Client
from blitzen import get_local_ip

address = (get_local_ip(), 57754)

def client():
    conn = Client(address, authkey=b'secret password')
    print(conn.recv())
    conn.close()

if __name__ == '__main__':
    client()