import grpc
import rpcs_pb2
import rpcs_pb2_grpc

class User:
    def __init__(self, ip, port, server_dict):
        self.channel = None
        self.stub = None
        self.ip = ip
        self.port = port
        self.server_conns = server_dict

    def connect(self, id):
        if id == -1:
            return
        self.channel = grpc.insecure_channel(f'{self.server_conns[id]}')
        self.stub = rpcs_pb2_grpc.RaftStub(self.channel)

    def set_val(self, key, value):
        request = rpcs_pb2.ServeClientRequest()
        request.type = 'set'
        request.variable = str(key)
        request.value = str(value)
        try:
         
            response = self.stub.ServeClient(request)
    
            if response.is_success:
                print("Value set successfully!")
            else:
                print(f'This node is not the leader! Try contacting Node with ID: {response.leader_id}')
                self.connect(response.leader_id)
        except grpc.RpcError as e:
            print('Server is unavailable!')
            print(e)

    def get_val(self, key) -> str:
        request = {'type' : 'get', 'variable': str(key), 'value' : ''}
        try:
            response = self.stub.ServeClient(rpcs_pb2.ServeClientRequest(**request))
            if response.is_success:
                return f"{response.response}"
            else:
                print(f'This node is not the leader! Try contacting Node with ID: {response.leader_id}')
                self.connect(response.leader_id)
        except grpc.RpcError as e:
            print('Server is unavailable!')
            print(e)

def main():
    # Provide the address where the node is hosted
    client_ip = '[::]'
    client_port = 12334

    server_conns = {0 : '[::]:49664', 1:'[::]:49665', 2:'[::]:49666'}
    client = User(client_ip, client_port, server_dict = server_conns)
    client.connect(0)

    while True:
        print("\nMENU:")
        print("1. Set a value")
        print("2. Read a value")
        print("3. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            key = input("Enter key: ")
            value = input("Enter value: ")
            client.set_val(key, value)

        elif choice == "2":
            key = input("Enter key: ")
            value = client.get_val(key)
            print(f"Value: {value}")

        elif choice == "3":
            print("Exiting...")
            break

        else:
            print("Invalid choice. Please enter a valid option.")

if __name__ == "__main__":
    main()



