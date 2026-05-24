class ProducerConfig:
    def __init__(self,producer_id,broker_id,sleepTime,topic_id):
        self.producer_id = producer_id
        self.broker_id = broker_id
        self.sleepTime = sleepTime
        self.message_id = 1
        self.topic_id = topic_id

Producer_List = [
    ProducerConfig("1","1",1,1),
    ProducerConfig("2","2",1,1),
    ProducerConfig("3","1",2,1)
]