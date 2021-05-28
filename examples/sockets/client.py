from multiprocessing.connection import Listener, Client

address = ('192.168.1.68', 50000)

def client():
    #Get port from endpoint
    conn = Client(address, authkey=b'secret password')
    port = conn.recv()
    conn.close()

    #Connect to port
    conn = Client((address[0], int(port)), authkey=b'secret password') #Connects to open port

    #Wait for task to complete
    delay = conn.recv()
    print(f'Task took {delay} seconds.')
    conn.close()

if __name__ == '__main__':
    client()