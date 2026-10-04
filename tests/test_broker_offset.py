from broker.broker import Broker

def test_broker_offset_store(tmp_path):
    broker=Broker(str(tmp_path))

    broker.create_topic("orders",3)

    # Commit offsets for different consumer groups
    broker.commit_offset("group1","orders",0,5)
    broker.commit_offset("group1","orders",1,10)
    broker.commit_offset("group2","orders",0,15)

    # Retrieve and assert offsets for group1
    offset_group1_partition0=broker.get_offset("group1","orders",0)
    offset_group1_partition1=broker.get_offset("group1","orders",1)

    assert offset_group1_partition0==5
    assert offset_group1_partition1==10

    # Retrieve and assert offsets for group2
    offset_group2_partition0=broker.get_offset("group2","orders",0)
    assert offset_group2_partition0==15