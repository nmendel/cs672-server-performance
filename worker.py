import threading
import logging
import time
import os
import hashlib
import binascii
    
from kafka import KafkaConsumer, KafkaProducer

KAFKA_CONNECTION = os.environ.get('KAFKA_CONNECTION')
ROUNDS = 100000

producer = KafkaProducer(bootstrap_servers=KAFKA_CONNECTION)

class SimpleConsumer(threading.Thread):

    daemon = True
                
    def run(self):

        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['simple1'])

        while True:
            for message in consumer:
                write_file(message.key, message.value, 'simple')

class ComplexConsumer(threading.Thread):
    daemon = True
                
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['complex1'])

        for message in consumer:
            data = bytes(message.value)
            use_some_cpu(data)
            producer.send('complex2', key=bytes(message.key), value=data)

class ComplexConsumer2(threading.Thread):
    daemon = True
                
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['complex2'])

        for message in consumer:
            data = bytes(message.value)
            use_some_cpu(data)
            producer.send('complex3', key=bytes(message.key), value=data)

class ComplexConsumer3(threading.Thread):
    daemon = True
                
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_CONNECTION)
        consumer.subscribe(['complex3'])

        while True:
            for message in consumer:
                data = bytes(message.value)
                use_some_cpu(data)
                write_file(message.key, message.value, 'complex')

def use_some_cpu(data):
    dk = hashlib.pbkdf2_hmac('sha256', data, b'saltysaltsalt', ROUNDS)
    return binascii.hexlify(dk)
                
def write_file(filename, data, req_type):
    path = os.path.join('/home/ubuntu/cs672-server-performance/files/', req_type, os.path.dirname(filename))
    print 'write file: %s' % os.path.join(path, os.path.basename(filename))
    if not os.path.exists(path):
        os.makedirs(path)
        
    fh = open(os.path.join(path, os.path.basename(filename)), 'w')
    fh.write(data)
    fh.close()
    
            
def main():
    threads = [
        SimpleConsumer(),
        ComplexConsumer(),
        ComplexConsumer2(),
        ComplexConsumer3()
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
