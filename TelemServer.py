import sys
import selectors
import socket
import types
from rethinkdb import RethinkDB

HOST = '127.0.0.1'
PORT = 65000

sel = selectors.DefaultSelector()

def accept_wrapper(sock):
    conn, addr = sock.accept()
    print('accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def serivce_connection(key, mask,r):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)
        if recv_data:
            #data.outb += recv_data
            rawData = str(recv_data.decode("utf-8"))
            parsedData = rawData.split(';')
            for data in parsedData:
                sp = data.split(':')
                if(len(sp) == 2):
                    r.db('test').table('Data').insert([{"name":sp[0], "value":sp[1]}]).run()
        else:
            print('closing connection to', data.addr)
            sel.unregister(sock)
            sock.close()

if __name__ == '__main__':
    r = RethinkDB()
    r.connect('localhost', 49154).repl()
    
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.bind((HOST, PORT))
    lsock.listen()
    print('Listening on ', (HOST, PORT))
    lsock.setblocking(False)
    sel.register(lsock,selectors.EVENT_READ, data=None)

    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj)
                else:
                    serivce_connection(key, mask,r)
    except KeyboardInterrupt:
        print('exiting now')
    finally:
        sel.close()
