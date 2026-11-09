from prometheus_client import Counter, Gauge, Histogram

#Broker health


broker_up=Gauge("broker_up", "Broker health status (1=up, 0=down)")


#Message activity

messages_produced=Counter("messages_produced_total", "Total number of messages produced to the broker",["topic","partition"])

messages_read=Counter("messages_read_total", "Total number of messages read from the broker",["topic","partition"])

#connections and consumers


active_connections=Gauge("active_connections", "Number of active TCP connections",["group","topic"])

active_consumers=Gauge("active_consumers", "Number of active consumers connected to the broker",["group","topic"])

#request latency

produce_latency=Histogram("broker_produce_latency_seconds", "Latency of produce requests in seconds",["topic"])

read_latency=Histogram("broker_read_latency_seconds", "Latency of read requests in seconds",["topic","partition"])


