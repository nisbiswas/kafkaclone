from .topic import Topic


class Broker:
    def __init__(self,log_dir):
        self.log_dir=log_dir
        self.topics={}

    def create_topic(self,topic_name,num_partitions):
        if topic_name in self.topics:
            raise ValueError(f"Topic '{topic_name}' already exists")

        topic_log_path=f"{self.log_dir}"
        topic=Topic(topic_name,num_partitions,topic_log_path)
        self.topics[topic_name]=topic
        return topic

    def get_topic(self,topic_name):
        return self.topics.get(topic_name)

    def append(self,topic_name,partition_id,message):
        topic=self.get_topic(topic_name)
        if not topic:
            raise ValueError(f"Topic '{topic_name}' does not exist")

        partition=topic.get_partition(partition_id)
        if not partition:
            raise ValueError(f"Partition '{partition_id}' does not exist in topic '{topic_name}'")

        return partition.append(message)

    def read(self,topic_name,partition_id,offset):

        topic=self.get_topic(topic_name)

        if not topic:
            raise ValueError(f"Topic '{topic_name}' does not exist")

        partition=topic.get_partition(partition_id)

        if not partition:
            raise ValueError(f"Partition '{partition_id}' does not exist in topic '{topic_name}'")

        return partition.read(offset)
    

    def has_offset(self,topic_name,partition_id,offset):
        topic=self.get_topic(topic_name)

        if not topic:
            raise ValueError(f"Topic '{topic_name}' does not exist")

        partition=topic.get_partition(partition_id)

        if not partition:
            raise ValueError(f"Partition '{partition_id}' does not exist in topic '{topic_name}'")

        return partition.has_offset(offset)