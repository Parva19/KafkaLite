import asyncio
from Broker.broker import Broker
from Producer.producer import Producer
from Producer.config import ProducerConfig
from Consumer.consumer import Consumer
from Consumer.config import ConsumerConfig

async def main():
    # ── 1. Create Broker ──
    broker = Broker()
    broker.addTopic("orders", num_partitions=3, topic_id=1)

    broker.registerConsumerGroup("group1", "orders",1)


    # ── 2. Create 10 Producers ──
    producer_configs = [
        ProducerConfig(
            producer_id=str(i),
            broker_id="1",
            sleepTime=1,
            topic_id=1
        )
        for i in range(1, 11)  # producer 1 to 10
    ]


     # create consumers and add to group
    consumers = []
    for i in range(1, 11):
        config = ConsumerConfig(
            consumer_id=str(i),
            broker_id="1",
            group_id="group1",
            topic_id=1
        )
        c = Consumer(config, broker)
        broker.addConsumerToGroup("group1", c)  # triggers rebalance
        consumers.append(c)

    # ── 3. Create 10 Consumers ──
    """consumer_configs = [
        ConsumerConfig(
            consumer_id=str(i),
            broker_id="1"
        )
        for i in range(1, 11)  # consumer 1 to 10
    ]
    """
    #topic="orders",
    #group_id="billing",

    # ── 4. Create Producer and Consumer objects ──
    producers = [Producer(config, broker) for config in producer_configs]
    #consumers = [Consumer(config, broker) for config in consumer_configs]

    # ── 5. Run all simultaneously ──
    producer_tasks = [p.run() for p in producers]
    consumer_tasks = [c.run() for c in consumers]

    await asyncio.gather(*producer_tasks, *consumer_tasks)

if __name__ == "__main__":
    asyncio.run(main())