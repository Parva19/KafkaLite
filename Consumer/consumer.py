from Consumer.config import ConsumerConfig
import time
from concurrent.futures import ThreadPoolExecutor
from Broker.broker import Broker
from Logger.logger import getLogger
import asyncio 

class Consumer:
    def __init__(self,config,broker=None):
        self.config = config
        self.broker = broker
        self.logger = getLogger("consumer")
        
    async def recieve_message(self):
        self.logger.info(f"Getting message for {self.config.consumer_id} from broker {self.config.broker_id}")
        message = f"consume {self.config.consumer_id} recieved the messsage form broker {self.config.broker_id} as - "
        ##go to broker and check if any messgae is there
        if self.broker:
            messages=await self.broker.consumeMessage(Consumer=self)
            if messages:
                for message in messages:
                    self.logger.debug(f"Received message: {message}")
                    self.logger.info(f"Received message: {message}")
            return messages
        else:
            self.logger.info(f"No messages for consumer {self.config.consumer_id}")
            return None
    
    def useMessage(self,message=None):
        self.logger.debug(f"Using message for {self.config.consumer_id}: {message}")
    
    async def run(self):
        while True:
            messages = await self.recieve_message()
            if messages:
                for message in messages:
                    self.useMessage(message)
            await asyncio.sleep(self.config.pollInterval)


async def startGettingMessage(consumer_list=None,broker=None):
    tasks = []
    for ithConsumer in consumer_list:
        p = Consumer(ithConsumer, broker)
        tasks.append(p.run())  # collect coroutines
    
    await asyncio.gather(*tasks)  # run all simultaneously

if __name__ == "__main__":
    #create a broker instance and pass it to producers
    #make a main function whre we create a broker instance and pass it to producers
    asyncio.run(startGettingMessage(Consumer_List))     