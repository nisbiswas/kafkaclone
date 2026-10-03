from broker.broker import Broker

def test_broker_create_topic():
    broker=Broker(log_dir="logs")

    topic_name="test_topic"
    num_partitions=3

    topic=broker.create_topic(topic_name,num_partitions)

    # Check if the topic is created and stored in the broker
    assert topic is not None
    assert broker.get_topic(topic_name)==topic

    # Check if the correct number of partitions are created
    assert len(topic.partitions)==num_partitions


def test_broker_append_and_read(tmp_path):
    log_dir=tmp_path/"logs"
    broker=Broker(log_dir=str(log_dir))

    topic_name="test_topic"
    num_partitions=2
    broker.create_topic(topic_name,num_partitions)

    # Append messages to the topic
    offset1=broker.append(topic_name,0,"Message 1")
    offset2=broker.append(topic_name,0,"Message 2")
    offset3=broker.append(topic_name,1,"Message 3")

    # Read messages from the topic
    assert broker.read(topic_name,0,offset1)=="Message 1"
    assert broker.read(topic_name,0,offset2)=="Message 2"
    assert broker.read(topic_name,1,offset3)=="Message 3"