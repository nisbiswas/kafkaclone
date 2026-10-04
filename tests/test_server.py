import socket
import threading

from broker.broker import Broker
from broker.server import handle_client

def start_test_server(host,port,broker):
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.bind((host,port))
    server.listen()
    port=server.getsockname()[1]

    def accept_client():
        conn,address=server.accept()
        thread=threading.Thread(target=handle_client,args=(conn,address,broker))
        thread.start()
    thread=threading.Thread(target=accept_client)
    thread.start()

    return server,port,thread

def send_request(sock,request):
    sock.sendall((request+"\n").encode("utf-8"))
    response=b""
    while b"\n" not in response:
        data=sock.recv(4096)
        if not data:
            raise ConnectionError("Server closed the connection")
        response+=data
    response=response.decode("utf-8").strip()
    return response

def test_fetch_offset_over_tcp(tmp_path):

    broker=Broker(log_dir=str(tmp_path))
    broker.create_topic("orders",num_partitions=3)

    server,port,thread=start_test_server("localhost", 0, broker)

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect(("localhost",port))

    response=send_request(sock,"FETCH_OFFSET group1 orders 0")
    assert response=="OK 0"

    sock.close()
    server.close()


def test_commit_offset_over_tcp(tmp_path):
    broker=Broker(log_dir=str(tmp_path))
    broker.create_topic("orders",num_partitions=3)

    server,port,thread=start_test_server("localhost", 0, broker)

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect(("localhost",port))

    response=send_request(sock,"COMMIT_OFFSET group1 orders 0 5")
    assert response=="OK"

    offset=broker.get_offset("group1","orders",0)
    assert offset==5

    sock.close()
    server.close()
