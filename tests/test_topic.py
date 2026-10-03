from broker.topic import Topic

def test_topic_partition_creation():
    topic_name="test_topic"
    num_partitions=3
    log_path="logs"

    topic=Topic(topic_name,num_partitions,log_path)

    # Check if the correct number of partitions are created
    assert len(topic.partitions)==num_partitions

    # Check if each partition is an instance of Partition
    for partition_id in range(num_partitions):
        partition=topic.get_partition(partition_id)
        assert partition is not None
        assert partition.partition_id==partition_id