class ConsumerConfig:
    def __init__(self,consumer_id,broker_id,group_id,topic_id=None):
        self.consumer_id = consumer_id
        self.broker_id = broker_id
        self.pollInterval = 1
        self.offset = 0
        self.group_id = group_id
        self.topic_id = topic_id
"""Consumer_List = [
    ConsumerConfig("1","1"),
    ConsumerConfig("2","2"),
    ConsumerConfig("3","1")
]"""