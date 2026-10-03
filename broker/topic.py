from .partition import Partition


class Topic:
    def __init__(self,topic_name,num_partitions,log_path):
        self.topic_name=topic_name
        self.partitions={}

        for partition_id in range(num_partitions):
            partition_log_path=f"{log_path}/{topic_name}/{partition_id}.log"
            self.partitions[partition_id]=Partition(partition_id,partition_log_path)

    def get_partition(self,partition_id):
        return self.partitions.get(partition_id)