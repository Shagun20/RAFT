import pickle
import random
import threading
import grpc 
import concurrent.futures
import threading
import math
import rpcs_pb2 as pb2
import rpcs_pb2_grpc as pb2_grpc
from rpcs_pb2 import Entry
import json
from threading import Timer
import time
import datetime

utc_tz = datetime.timezone.utc
# source : https://github.com/aalexren/iu-dnp/blob/master/week07

    
class Node():
    def __init__(self, id, ip, port, conns = []):
        self.id = id

        self.ip = ip
        self.port = port

        self.state = 'FOLLOWER' 
        # self.term =0
        self.cur_leader_id = -1

        self.election_timeout = random.uniform(a = 5.0, b = 10.001)
        self.election_timer = None

        self.LEASE_INTERVAL = datetime.timedelta(seconds = 2)
        self.leader_lease_timestamp = datetime.datetime.now(utc_tz)
        self.lease_timer = None
        self.has_lease = False
        # conns is a dict of id and address of the nodes
        self.conns = conns
        self.commitIndex = 0
        self.lastApplied = 0
        self.peer_channels = {}

        # logs is list of tuples with update and term
        # [ string command, term])
        self.logs = []

        # inititalise these values for each node as it becomes a leader
        self.nextIndex = []
        # inititalise these values for each node as it becomes a leader
        self.matchIndex = []

        # committed entries is committed entries dict
        self.committed_entries = {}
        # made dict committed logs {} with key as key and value as [value, term]
        self.committed_logs= {}

        self.init_meta_data()
        # self.init_logs()
        self.reset_election_timer()
                

    def init_meta_data(self):
        try:
            with open(f'meta_{self.id}.txt', 'r') as meta:
                meta.read()
        except:
            with open(f'meta_{self.id}.txt', 'w') as meta:
                meta.write(f'node_id -1 \n')
                meta.write('term 0 \n')
                meta.write('commit_len 0 \n')

    def init_dump(self):
        with open(f'dump_{self.id}.txt', 'w') as f:
            f.write('Initial content')
    
    def add_to_dump(self, command):
        prev_commands = open(f'dump_{self.id}.txt', 'r').readlines()
        prev_commands.append(command)
        with open(f'dump_{self.id}.txt', 'w') as f:
            f.writelines(prev_commands)
                
    # def init_commit_logs(self):
    #     try:
    #         with open(f'committed_logs_{self.id}.json', 'r') as f:
    #             self.committed_logs = json.load(f)
    #     except:
    #         with open(f'committed_logs_{self.id}.txt', 'w') as f:
    #             json.dump(self.committed_logs, f)
    

    
    def change_metadata(self, type:str, value = None) -> int:

        meta_data = open(f'meta_{self.id}.txt', 'r').readlines()

        if type.lower() == 'node': #change nodeId 

            if value:
                meta_data[0] = meta_data[0][ :len('node_id ')] + str(value) + '\n'

        elif type.lower() == 'term': #change the term

            cur_term = int(meta_data[1][len('term '):])+1

            if value:
                meta_data[1] = str(meta_data[1][:len('term ')]) + str(value) + ' \n'

            else:
                meta_data[1] = str(meta_data[1][:len('term ')]) + str(cur_term) + ' \n'

        else: #change the commit length
            cur_commit_len = int(meta_data[2][len('commit_len '):])+1

            if not value:
                meta_data[2] = str(meta_data[2][:len('commit_len ')]) + str(cur_commit_len) + ' \n'
            
            else:
                meta_data[2] = str(meta_data[2][:len('commit_len ')]) + str(value) + ' \n'

        try:

            with open(f'meta_{self.id}.txt', 'w') as meta:
                meta.writelines(meta_data)
            return 0
        
        except:
            return -1
    

    def get_metadata(self, type:str):

        meta_data = open(f'meta_{self.id}.txt', 'r').readlines()

        if type.lower() == 'node':

            node_id = int(meta_data[0][len('node_id '):])
            return node_id
        
        elif type.lower() == 'term':

            term = int(meta_data[1][len('term '):])
            return term
        
        else:

            commit_len = int(meta_data[2][len('commit_len '):])
            return commit_len
    
    
    
    # def init_logs(self): #initialise logs_node_id.json

    #     try:

    #         with open(f'logs_{self.id}.json', 'r') as f:
    #             self.logs = json.load(f)

    #     except:

    #         with open(f'logs_{self.id}.json', 'w') as f:
    #             json.dump(self.logs, f)
    

    # def get_logs(self): 

    #     with open(f'logs_{self.id}.json', 'r') as f:
    #         self.logs = json.load(f)
        

    #     return self.logs

    def write_to_logs(self):
        
        with open(f'logs_{self.id}.txt', 'w') as f:

            for entry in self.logs:
                f.write(f"Term: {entry['term']}, Update: {entry['update']}\n")
        return

    
    # def commit_log(self): #use this if you want to commit all the entries present in self.append_entries

    #     self.logs = self.get_logs() #get from storage
    #     self.logs.extend(self.append_entries) # add these entries

    #     self.write_to_logs() # write to the storage file

    #     self.logs = self.get_logs() #use the updated logs
    #     self.append_entries = [] # reinitialise to empty list after all commits
        
    # def get_committed_logs(self): # always use this before using the persistent self.logs

    #     with open(f'committed_logs_{self.id}.json', 'r') as f:
    #         self.committed_logs = json.load(f)

    #     return self.committed_logs

    # def write_to_committed_logs(self):

    #     with open(f'committed_logs_{self.id}.json', 'w') as f:
    #         json.dump(self.committed_logs, f, indent=4)

    #     return
    def init_commit_logs(self):
        try:
            with open(f'committed_logs_{self.id}.txt', 'r') as f:
                lines = f.readlines()
                self.committed_logs = {}
                for line in lines:
                    key, value, term = line.strip().split(',')
                    self.committed_logs[key] = [value, int(term)]
        except FileNotFoundError:
            with open(f'committed_logs_{self.id}.txt', 'w') as f:
                f.write(self.committed_logs)

            

    def get_committed_logs(self):
        self.init_commit_logs()  # Ensure committed logs are initialized
        return self.committed_logs

    def write_to_committed_logs(self):
        with open(f'committed_logs_{self.id}.txt', 'w') as f:
            for key, (value, term) in self.committed_logs.items():
                f.write(f"{key},{value},{term}\n")

    
    def get_prev_log_term(self):
       
        try:
            return self.logs[-1]['term']
        except:
            return 0
    
    def get_prev_log_index(self):
       
        try:
            return len(self.logs)
        except:
            return 0

    def reset_election_timer(self):
        print(f'Node with id: {self.id} resetting its election timer')
        if self.election_timer:
            self.election_timer.cancel()

        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        # waits for election_timeout seconds, after which it runs start_election()
        self.election_timer.start()
    
    def cancel_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()

    def reset_lease_timer(self):
        # similar logic to election timer

        if self.state.lower() != 'leader':
            return
        if self.lease_timer:
            self.lease_timer.cancel()
        
        td = datetime.timedelta(seconds = 10)
        self.leader_lease_timestamp = datetime.datetime.now(utc_tz) + td
        self.lease_timer = threading.Timer(10, self.become_follower, args = [self.get_metadata('term')])
        self.lease_timer.start()


    def get_lease_duration(self):
        # if self.state.lower() != 'leader':
        #     return 0
        
        new_lease_interval = (self.leader_lease_timestamp - datetime.datetime.now(utc_tz)).total_seconds()
        return max(0, new_lease_interval)
  

    def start_election(self): # for client  


        print(f'Node {self.id} election timer timed out, Starting election.')
        self.add_to_dump(f'Node {self.id} election timer timed out, Starting election.')
        self.state = 'CANDIDATE'
        
        new_term = self.get_metadata('term') + 1
        #update the new term


        self.change_metadata('node', self.id) # voted for self
        self.change_metadata('term', new_term) # change the term
        # first change in the meta.txt then use them for rpc ops

        self.reset_election_timer() # so that if leader is not chosen within this time it again becomes candidate

        #make the RPC request
        request_vote_req = pb2.RequestVoteRequest()

        request_vote_req.candidate_id = self.id
        request_vote_req.term = new_term
        
        request_vote_req.last_log_term = self.get_prev_log_term()
        request_vote_req.last_log_index = self.get_prev_log_index()

        votes_recvd = 1

        

        #use for waiting until old leader lease expires
        max_lease_time = max(0, (self.leader_lease_timestamp - datetime.datetime.now(utc_tz)).total_seconds())
        responses = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.conns)) as executor:

            futures = []
            
            for id in self.conns:
                
                #reuse channels wherever possible
                
                channel = self.peer_channels.get(id, grpc.insecure_channel(self.conns[id]))

                self.peer_channels[self.conns[id]] = channel
              

                try:
                    #waits for the channel to be ready 
                    grpc.channel_ready_future(channel).result(timeout=0.2)
                    
                except grpc.FutureTimeoutError as e:
                    print(e)
                    pass

                stub = pb2_grpc.RaftStub(channel)

                try:
                    future = executor.submit(stub.RequestVote, request_vote_req)
                    futures.append(future)

                except Exception as e:
                    # if error from RPC call continue (handles cases of sending rpc to dead node)
                    print(e)
                    continue

            for future in concurrent.futures.as_completed(futures):

                if future.exception() is None:
                    # if no error, then use the response to identify if candidate is leader
                    response = future.result()
                    
                    responses.append(response)  
                else:
                    print(future.exception())
        
        for response in responses:
            # Now After getting responses go through them
            if response is None:
                continue

            if new_term < response.term: # stale node hence become follower 
                    self.become_follower(response.term)
                    return
                
            elif new_term == response.term:
                resp_lease_left = response.lease_timer # get the timestamp according to response from other node

                max_lease_time = max(resp_lease_left, max_lease_time) #update max_lease timestamp 

                if response.vote_granted:
                    votes_recvd+=1
                if votes_recvd*2 > len(self.conns)+1:
                    # if you get majority become leader 

                    print('New Leader waiting for Old Leader Lease to timeout.')
                    self.add_to_dump('New Leader waiting for Old Leader Lease to timeout.')
                    # wait for max lease time to elapse
                    time.sleep(max(0, max_lease_time))


                    self.start_leader()
                    return

        self.reset_election_timer()

    def start_leader(self):

        print(f'King of the World {self.id}')
        self.state = 'LEADER'
        self.has_lease = True

        self.cancel_election_timer()
        self.reset_lease_timer()
        self.matchIndex = [0]*(len(self.conns)+1)
        self.nextIndex = [len(self.logs)+1]*(len(self.conns)+1)
        self.cur_leader_id = self.id
        no_op_entry = {
            'term' : self.get_metadata('term'),
            'update' : ['NO-OP']
        }
        self.logs.append(no_op_entry)
        self.matchIndex[self.id] +=1
        self.nextIndex[self.id]+=1
        self.write_to_logs()
        self.heartbeat_setter()
        print(f"Node {self.id} became the leader for term {self.get_metadata('term')}.")
        self.add_to_dump(f"Node {self.id} became the leader for term {self.get_metadata('term')}.")
        

        return
            

    def become_follower(self, term):
        if self.state.lower() == 'leader':
            print(f"{self.id} Stepping down")
            self.add_to_dump(f"{self.id} Stepping down")
           
        print(f'Node with id: {self.id} has become a follower')
        self.reset_election_timer()
        self.state = 'FOLLOWER'
        self.cur_leader_id = -1
        
        self.has_lease = False
        self.change_metadata('node', -1) # has not voted for anybody right now
        self.change_metadata('term', term) #change the term


    def get_val(self, var) -> tuple:
        
        self.committed_logs = self.get_committed_logs() # change for commited logs here

        value = self.committed_logs.get(var, [None])

        return str(value[0])

    def set_val(self, variable, value):
        command = ['SET', variable ,value]


        term = self.get_metadata('term')

        # 
        self.logs.append({'update': command, 'term': term})
        self.write_to_logs()
        return
    
    
    def send_entries(self, peer):

        if self.state.lower() == 'leader':

            print(f'Leader {self.id} sending heartbeat & Renewing Lease')
            self.add_to_dump(f'Leader {self.id} sending heartbeat & Renewing Lease')
            # creating a stub for sending the requests
            channel = self.peer_channels.get(self.conns[peer], grpc.insecure_channel(f'{self.conns[peer]}'))
            stub = pb2_grpc.RaftStub(channel)
            append_entries_req = pb2.AppendEntriesRequest()
            
            self.term=self.get_metadata('term')
            append_entries_req.leaderTerm = self.term
            append_entries_req.leaderId = self.id
            # min val of prevLogIndex is -1
    
            append_entries_req.prevLogIndex = self.nextIndex[peer] - 1 
            # if self.nextIndex[peer] > 0 else -1
            #if prevLogIndex is -1 then setting prevLogTerm to 0
            if self.nextIndex[peer] >=1:
                append_entries_req.prevLogTerm = self.logs[self.nextIndex[peer]-1]['term'] 

            # Checking if there are previous entries to be sent
            if self.nextIndex[peer] <= len(self.logs):
                # Concatenating previously unappended entry
                

                # print([self.logs[self.nextIndex[peer] - 1]])
                try:
                    entry_dict = self.logs[self.nextIndex[peer] - 1]

                    custom_entry = Entry()
                    custom_entry.term = entry_dict['term']
                    custom_entry.update.extend(entry_dict['update'])
                    append_entries_req.entries.append(custom_entry) 
                    # print("printing entry",append_entries_req.entries ) 
                    
                except Exception as e:
                    pass

            else:
                # If no new entries, sending heartbeat with empty entries
                append_entries_req.entries =[]
    
            append_entries_req.leaderCommit = self.commitIndex
     
            # to let the followers know about remaining time of lease
          
            append_entries_req.leaseDuration = self.get_lease_duration()

            

            try:
                print('receiving responses...')
                response = stub.AppendEntries(append_entries_req)
                
                print("resoponse status",response.success)
                
                if response.term > self.get_metadata("term"):
                    # If term in response is greater, leader becomes follower and updates the term
                    self.become_follower(response.term)
                    print(f'{self.id} Stepping down')
                    print(f"I am a follower now with Term: {response.term}")
                else:
                    # details of entries replicated on the follower server
                    
                    if response.success :
                        print("successful response")
                        if self.nextIndex[peer] <=len(self.logs):
                            self.matchIndex[peer] = self.nextIndex[peer]
                            self.nextIndex[peer] += 1
                            
                    else:
                        print("unsuccessful response")
                        self.nextIndex[peer] -= 1
                        # max(self.nextIndex[peer] - 1, 1)
                        # check if -1 or 0
                        self.matchIndex[peer] = min(self.matchIndex[peer], self.nextIndex[peer] - 1)

            except Exception as e:
                print("Found Exception: ",e)
                print(f'Error occurred while sending RPC to Node {peer}')
                self.add_to_dump(f'Error occurred while sending RPC to Node {peer}')
        else:
            print("Not a leader, cannot send heartbeat")


    def heartbeat_setter(self):

        if self.state.lower() != 'leader':
            return
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.conns))
        for peer in self.conns.keys():
            # Sending heartbeat to neighbours/followers
            if peer != self.id: 
                # sending the id of the peer 
                print(f"peer is {peer} ")
                pool.submit(self.send_entries,peer)

        # details of entries replicated on the leader server
        self.matchIndex[self.id] = len(self.logs) 
        self.nextIndex[self.id] = self.matchIndex[self.id] + 1
        
        supportNodes = 0
        for peer_len in self.matchIndex:
            if peer_len >= self.commitIndex +1 :
                supportNodes += 1
    
     
        print('support nodes if new entry', supportNodes)
        if supportNodes > len(self.matchIndex)//2:
            self.commitIndex += 1
            self.reset_lease_timer()
            print(f"Leader {self.id}'s lease timer has reset.")
            
        else:
            print(f' ACKS recvd if any new entry: {supportNodes}')
            # print(f"Leader {self.id} lease renewal failed.")


        while self.lastApplied < self.commitIndex :
           
            print(f' Last applied is {self.lastApplied}')
            command_parts = self.logs[self.lastApplied]['update'] # [set, key, value]
            if command_parts[0].lower() == 'set':           
                key = command_parts[1]
                value = command_parts[2]
                self.committed_entries[key] = value
                self.lastApplied += 1
                self.committed_logs[key]=[value, self.logs[self.lastApplied]['term']]
                self.write_to_committed_logs()
                print(f"Node {self.id} (leader) committed the entry {[command_parts[0], key, value, self.logs[self.lastApplied]['term']]} to the state machine.")
                self.add_to_dump(f"Node {self.id} (leader) committed the entry {[command_parts[0], key, value, self.logs[self.lastApplied]['term']]} to the state machine.")
            else:
                self.lastApplied += 1
                # print(f"Node {self.id} (leader) sent the entry {[command_parts[0],self.logs[self.lastApplied]['term']]} to the state machine.")
                # self.add_to_dump(f"Node {self.id} (leader) committed the entry {[command_parts[0], self.logs[self.lastApplied]['term']]} to the state machine.")
        # send periodic heartbeats 
        self.heartbeat = Timer(1, self.heartbeat_setter)
        self.heartbeat.start()

# this is PSEUDOCODE
        
    # set self.lease_ts = datetime.datetime.now(utc_tz) in the beginning
    def helper(self, request, context):
        # use it for lease time updates for followers
        #in appendlogs:
        new_delta = datetime.timedelta(seconds = max(0, request.new_lease_interval)) #used for 
        self.lease_ts = datetime.datetime.now(utc_tz) + new_delta

    def send_append_logs(self): # for leader

        new_lease_interval = (self.lease_ts - datetime.datetime.now(utc_tz)).total_seconds()


    def recv_append_logs(self): # for leader
        num_acks, num_servers = 5, 9
        if 2*num_acks > num_servers: # if majority return append_logs ack

            self.lease_ts = datetime.datetime.now(utc_tz) + self.LEASE_INTERVAL # extend the lease
    


class ServerHandler(pb2_grpc.RaftServicer, Node):
    def __init__(self, id, ip, port):
        super().__init__(id, ip, port)
        print(f'The server with id: {self.id} has started at {self.ip}:{self.port}')
        cur_term = self.get_metadata('term')
        print(f'This server is a follower. Term = {cur_term}')
        self.init_dump()

    def RequestVote(self, request, context): # for servers
        self.reset_election_timer()

        response = pb2.RequestVoteResponse()
        #default value 
        
        response.vote_granted = False
        
        response.lease_timer = (self.leader_lease_timestamp - datetime.datetime.now(utc_tz)).total_seconds()

        cand_term = request.term
        cand_node = request.candidate_id
        cand_log_index = request.last_log_index
        cand_log_term = request.last_log_term

        cur_term = self.get_metadata('term')

        if cur_term < cand_term:

            self.become_follower(cand_term)

        cur_term = self.get_metadata('term')
        cur_node = int(self.get_metadata('node'))

        cur_log_index = self.get_prev_log_index()
        cur_log_term = self.get_prev_log_term()

        if cur_term > cand_term:
            response.term = cur_term
            
        
        elif cur_term == cand_term and (cur_node == -1 or cur_node == cand_node):

            response.term = cur_term
            
            # only Up to date candidate is voted

            if cand_log_term == cur_log_term:

                response.vote_granted = cand_log_index >= cur_log_index

            elif cand_log_term > cur_log_term:
                response.vote_granted = True
            
            if response.vote_granted:
                self.change_metadata('node', value = cand_node)
                self.change_metadata('term', value = cand_term)
        
                print(f"Vote granted for Node {cand_node} in term {cand_term}.")
                self.add_to_dump(f"Vote granted for Node {cand_node} in term {cand_term}.")
            else:
                print(f"Vote denied for Node {cand_node} in term {cand_term}.")
                self.add_to_dump(f"Vote denied for Node {cand_node} in term {cand_term}.")
        return response
    
    def AppendEntries(self, request, context):
        term = request.leaderTerm
        leader_id = request.leaderId
        
        prevLogIndex = request.prevLogIndex
        prevLogTerm = request.prevLogTerm
        remaining_lease_time = request.leaseDuration
        entries = request.entries
        leaderCommit = request.leaderCommit

        if self.get_metadata('term') < term:
            self.change_metadata('term', term) 
            # self.become_follower(term)
            li = datetime.timedelta(seconds=remaining_lease_time)
            self.leader_lease_timestamp = datetime.datetime.now(utc_tz) + li
            self.cur_leader_id = leader_id
         

        if self.get_metadata('term') <= term:
            self.success = True
    
        else:
            self.success = False
          
        # follower cannot accept the new log entries because its log does not match the leader's 
        if len(self.logs) < prevLogIndex:
            self.success = False

            # print(f'Node {self.id} rejected AppendEntries RPC from Leader {leader_id}: Previous log index out of range')

        if prevLogIndex <= len(self.logs):
            # Truncating the logs if the leader's log is behind follower's log
            # if self.logs[prevLogIndex - 1][1] != prevLogTerm:
            #     self.success = False
            #     # print(f'Node {self.id} rejected AppendEntries RPC from Leader {leader_id}: Previous log term does not match')
            # else:
            self.logs = self.logs[:prevLogIndex]
        
                    

        if len(entries) > 0: 
            entry = entries[0]
            term = entry.term
            update = entry.update
            entry_recv = {'term': term,'update': update}
          
            if entry_recv not in self.logs:
         
                self.logs.append(entry_recv)
     
                self.write_to_logs()
           

    
        if self.success:
            print(f'Node {self.id} accepted AppendEntries RPC from {leader_id}.')
            self.add_to_dump(f'Node {self.id} accepted AppendEntries RPC from {leader_id}.')
        else:
            print(f'Node {self.id} rejected AppendEntries RPC from {leader_id}.')
            self.add_to_dump(f'Node {self.id} rejected AppendEntries RPC from {leader_id}.')

        if self.commitIndex < leaderCommit:
            # Updating commit index based on the leader's commit index
            self.commitIndex = min(len(self.logs), leaderCommit)

        # Applying committed entries
            while self.lastApplied < self.commitIndex:
                
                command_parts = self.logs[self.lastApplied]['update'] 
                if command_parts[0] == 'SET': 
                    command_parts = self.logs[self.lastApplied]['update']
                    key = command_parts[1]
                    value = command_parts[2]
                    self.committed_entries[key] = value
                    self.lastApplied += 1
                    self.committed_logs[key] = [value, self.logs[self.lastApplied]['term']]
                  
                    self.write_to_committed_logs()
                    
                    print(f"Node {self.id} (follower) committed the entry {[command_parts[0], key, value, self.logs[self.lastApplied]['term']]} to the state machine.")
                    self.add_to_dump(f"Node {self.id} (follower) committed the entry {[command_parts[0], key, value, self.logs[self.lastApplied]['term']]} to the state machine.")
                else:   
                    self.lastApplied += 1
                    
                 

        response = pb2.AppendEntriesResponse(term=self.get_metadata('term'), success=self.success)
        
        return response
     


    def ServeClient(self, request, context):
        response = pb2.ServeClientResponse()

        response.is_success = False
        response.leader_id = self.cur_leader_id
        response.response = ''

        if self.state != 'LEADER':
            return response
        print(f'Node {self.id} (leader) received an {request.type} request')
        self.add_to_dump(f'Node {self.id} (leader) received an {request.type} request')
        if request.type == 'get':

            # if not self.has_lease: #check if still leader
                # return response
            
            value = self.get_val(request.variable)

            if value == 'None':
                return response
            
            response.response = value
            response.is_success = True

            return response
        
        elif request.type == 'set': 

            # if not self.has_lease: #check if still leader
                # return response
            
            self.set_val(request.variable, request.value)
            response.is_success = True
            return response

            