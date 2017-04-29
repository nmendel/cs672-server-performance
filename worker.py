import threading, logging, time, os

from kafka import KafkaConsumer, KafkaProducer

KAFKA_CONNECTION = os.environ.get('KAFKA_CONNECTION')

producer = None

class SimpleProducer(threading.Thread):
    daemon = True

    def __init__(self):
        producer = KafkaProducer(bootstrap_servers=KAFKA_CONNECTION)
    
    def send(self, key, value, queue):
        producer.send(queue, key=bytes(key), value=bytes(value))
            
class SimpleConsumer(threading.Thread):
    daemon = True
                
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['simple1'])

        for message in consumer:
            print 'simple write: ' + message.key
            fh = open(os.path.join('/home/ubuntu/files/simple/', message.key), 'w')
            fh.write(message.value)
            fh.close()

class ComplexConsumer(threading.Thread):
    daemon = True
                
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['complex1'])
        
        for message in consumer:
            producer.send(message.key, message.value, 'complex2')

            
def main():

    producer = SimpleProducer()
    
    threads = [
        #Producer(),
        SimpleConsumer(),
        ComplexConsumer()
    ]

    for t in threads:
        t.start()

    while True:
        time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
