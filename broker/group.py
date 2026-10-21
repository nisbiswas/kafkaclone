import threading
import time

class ConsumerGroup:

    def __init__(self,group_id,topic_name,num_partitions):
        
        self.group_id=group_id
        self.topic_name=topic_name
        self.num_partitions=num_partitions

        self.members=set()
        self.assignments={}

        self.lock=threading.Lock()

        self.last_heartbeat={}
        self.session_timeout=15

    def join(self,consumer_id):
               
        with self.lock:
            self.members.add(consumer_id)

            self.last_heartbeat[consumer_id]=time.monotonic()
            self.rebalance()
            return self.assignments[consumer_id]

    def leave(self,consumer_id):
        with self.lock:
            self.members.discard(consumer_id)
            self.last_heartbeat.pop(consumer_id,None)
            self.rebalance()

    def get_assignment(self,consumer_id):
        with self.lock:
            return self.assignments.get(consumer_id,[]).copy()
        
## implementting self rebalance using round robin 
   
    def rebalance(self):
        if not self.members:
            self.assignments={}
            return

        sorted_members=sorted(self.members)
        assignments={member:[] for member in sorted_members}

        for partition_id in range(self.num_partitions):
            member_index=partition_id%len(sorted_members)
            member=sorted_members[member_index]
            assignments[member].append(partition_id)

        self.assignments=assignments


    def heartbeat(self,consumer_id):
        with self.lock:
            if consumer_id not in self.members:
                raise ValueError(
                    f"Consumer '{consumer_id}' is not part of the group '{self.group_id}'"
                )
            self.last_heartbeat[consumer_id]=time.monotonic()
            return self.assignments.get(consumer_id,[]).copy()

    def remove_expired_consumers(self):
        with self.lock:
            curr_time=time.monotonic()
            expired_consumers=[]

            for consumer_id,last_seen in self.last_heartbeat.items():
                if curr_time-last_seen>self.session_timeout:
                    expired_consumers.append(consumer_id)

            if not expired_consumers:
                return []

            for consumer_id in expired_consumers:
                self.members.discard(consumer_id)
                self.last_heartbeat.pop(consumer_id,None)

            self.rebalance()

            return expired_consumers