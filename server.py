from flask import Flask, request
from kafka import KafkaProducer
import os
import random
import time
import pdb

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_CONNECTION'))

files = []

@app.route('/simple', methods=['POST'])
def simple():
        filename = write_to_queue('simple1')
        return wait_and_rm(filename, 'simple')

@app.route('/complex', methods=['POST'])
def complex():
        filename = write_to_queue('complex1')
        return wait_and_rm(filename, 'complex')

def write_to_queue(queue):
        filename = random.choice(files)
        fh = open(filename, 'r')
        data = fh.read()
        fh.close()
        producer.send(queue, key=bytes(filename), value=bytes(data))
        return filename

def wait_and_rm(filename, req_type):
        filepath = os.path.join('/home/ubuntu/cs672-server-performance/files/', req_type, filename)

        attempts = 0
        while True:
                if os.path.exists(filepath):
                        print 'unlink %s' % filepath
                        os.unlink(filepath)
                        return '200'

                if attempts > 100:
                        return '504'
                
                attempts += 1
                time.sleep(.1)

if __name__ == '__main__':
        for (dirpath, dirnames, filenames) in os.walk('files/etc'):
                files.extend([os.path.join(dirpath, fil) for fil in filenames])

        app.run(host='0.0.0.0')
