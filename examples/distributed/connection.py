from multiprocessing.connection import Listener, Client

address = ('192.168.1.2', 50000)

def client():
    #Get port from endpoint
    conn = Client(address, authkey=b'authkey')
    conn.send((None, 'dummy', (), {}))
    port = conn.recv()
    conn.close()
    print(port)

    #Connect to port
    # conn = Client((address[0], int(port)), authkey=b'secret password') #Connects to open port

    # #Wait for task to complete
    # delay = conn.recv()
    # print(f'Task took {delay} seconds.')
    # conn.close()

if __name__ == '__main__':
    client()