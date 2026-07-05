import grpc
from main_server import ServerHandler
import rpcs_pb2
import rpcs_pb2_grpc as rpcs_pb2_grpc 
import time

import concurrent.futures

def run_server(handler: ServerHandler):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers= 10))
    rpcs_pb2_grpc.add_RaftServicer_to_server(handler, server)
    server.add_insecure_port(f'{handler.ip}:{handler.port}')
    try:
        server.start()
        server.wait_for_termination()
    except:
        exit()

new_server = ServerHandler(2, '[::]', 49666)
new_server.conns = {0:'[::]:49664', 1:'[::]:49665'}
try:
    run_server(new_server)
except Exception as e:
    print(e)