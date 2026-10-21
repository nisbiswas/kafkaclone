

import time

from broker.group import ConsumerGroup



def test_expired_consumer_rebalance():

    group=ConsumerGroup("group-1","orders",3)

    group.join("consumer-1")
    group.join("consumer-2")

    assert group.get_assignment("consumer-1")==[0,2]
    assert group.get_assignment("consumer-2")==[1]