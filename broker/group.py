import threading


class ConsumerGroup:

    def __init__(self,group_id,topic_name,num_partitions):
        
        self.group_id=group_id
        self.topic_name=topic_name
        self.num_partitions=num_partitions

        self.members=set()
        self.assignments={}

        self.lock=threading.Lock()

    def join(self,consumer_id):
               
        with self.lock:
            self.members.add(consumer_id)
            self.rebalance()
            return self.assignments[consumer_id]

    def leave(self,consumer_id):
        with self.lock:
            self.members.discard(consumer_id)
            self.rebalance()

    def get_assignment(self,consumer_id):
        with self.lock:
            return self.assignments.get(consumer_id,[])
        
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
