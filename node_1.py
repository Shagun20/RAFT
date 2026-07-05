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
new_server = ServerHandler(0, '[::]', 49664)
new_server.conns = {1:'[::]:49665', 2:'[::]:49666'}

try:
    run_server(new_server)
except Exception as e:
    print(e)



# def test_append_entries():
#     # add_server=
#     new_server = ServerHandler(1, '[::]', 49664)

#     # add_server=
#     new_server.conns = {2:'[::]:49665', 3:'[::]:49666', 4:'[::]:49667'}
#     try:
#         run_server(new_server) 
#     except Exception as e:
#         print(e)
#     # Initialize nodes
#     nodes = new_server.conns

#     # for i in range(1, 6):
        
#     #     node = Node(id=i, ip='localhost', port=5000 + i, conns=[f'localhost:{5000 + j}' for j in range(1, 6) if j != i])
#     #     nodes.append(node)

#     # Simulate leader election
#     nodes[0].start_leader()
#     leader_id = nodes[0].id

#     # Wait for NO-OP entry to be appended in all logs
#     time.sleep(1)  # Adjust sleep time as needed

#     # Test Case 1: Have the client perform 3 set requests and then 3 get requests
#     print("Test Case 1: Have the client perform 3 set requests and then 3 get requests")
#     for i in range(3):
#         variable = f"variable{i}"
#         value = f"value{i}"
      
#         nodes[0].set_val(variable, value, nodes)


#     time.sleep(1)  # Wait for entries to be replicated

#     for i in range(3):
#         variable = f"variable{i}"
#         error, value = nodes[0].get_val(variable)
#         if not error:
#             print(f"GET request for variable {variable} returned value: {value}")
#         else:
#             print(f"GET request for variable {variable} failed")

#     # # Test Case 2: Terminate one or two of the follower nodes and perform 3 SET requests and 3 GET requests
#     # print("\nTest Case 2: Terminate one or two of the follower nodes and perform 3 SET requests and 3 GET requests")
#     # for node in nodes[1:3]:
#     #     # Terminate follower nodes
#     #     print(f"Terminating node {node.id}")
#     #     # Simulate node termination

#     # for i in range(3):
#     #     variable = f"variable{i+3}"
#     #     value = f"value{i+3}"
#     #     nodes[0].set_val(variable, value)

#     # time.sleep(1)  # Wait for entries to be replicated

#     # for i in range(3):
#     #     variable = f"variable{i+3}"
#     #     error, value = nodes[0].get_val(variable)
#     #     if not error:
#     #         print(f"GET request for variable {variable} returned value: {value}")
#     #     else:
#     #         print(f"GET request for variable {variable} failed")

#     # for node in nodes[1:3]:
#     #     # Restart terminated follower nodes
#     #     print(f"Restarting node {node.id}")
#     #     # Simulate node restart

#     # # Test Case 3: Terminate the current leader process and restart
#     # print("\nTest Case 3: Terminate the current leader process and restart")
#     # print(f"Terminating current leader (Node {leader_id})")
#     # # Simulate leader termination

#     # # Wait for leader election
#     # time.sleep(1)  # Adjust sleep time as needed

#     # new_leader_id = None
#     # for node in nodes:
#     #     if node.state == 'LEADER':
#     #         new_leader_id = node.id
#     #         break

#     # if new_leader_id:
#     #     print(f"New leader elected: Node {new_leader_id}")
#     # else:
#     #     print("No new leader elected")

#     # # Restart terminated leader
#     # print(f"Restarting terminated leader (Node {leader_id})")
#     # # Simulate leader restart

#     # # Test Case 4: Terminate majority follower nodes before lease timeout
#     # print("\nTest Case 4: Terminate majority follower nodes before lease timeout")
#     # for node in nodes[3:]:
#     #     # Terminate majority follower nodes
#     #     print(f"Terminating node {node.id}")
#     #     # Simulate node termination

#     # # Wait for lease timeout
#     # time.sleep(10)  # Wait for lease to timeout

    # # Verify leader steps down
    # new_state = nodes[0].state
    # if new_state == 'FOLLOWER':
    #     print("Leader stepped down successfully")
    # else:
    #     print("Leader did not step down")

    # # Test Case 5: Terminate all nodes except two followers and send requests
    # print("\nTest Case 5: Terminate all the nodes except two follower nodes and send requests")
    # for node in nodes[2:]:
    #     # Terminate all nodes except two followers
    #     print(f"Terminating node {node.id}")
    #     # Simulate node termination

    # # Send GET and SET requests to one of the followers
    # # Simulate requests
    # time.sleep(1)  # Wait for requests to fail

    # print("Requests failed as there is no leader present in the system")

# if __name__ == "__main__":
#     # test_append_entries()
#     run_server