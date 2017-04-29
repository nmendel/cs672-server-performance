from flask import Flask, request
from kafka import KafkaProducer
import os
import random
import pdb

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_CONNECTION'))

files = []

@app.route('/simple', methods=['POST'])
def simple():
        return write_to_queue('simple1')

@app.route('/complex', methods=['POST'])
def complex():
        return write_to_queue('complex1')

def write_to_queue(queue):
        filename = random.choice(files)
        fh = open(filename, 'r')
        data = fh.read()
        fh.close()
        producer.send(queue, key=bytes(filename), value=bytes(data))
        return '200'

    
if __name__ == '__main__':
        for (dirpath, dirnames, filenames) in os.walk('/etc'):
                files.extend([os.path.join(dirpath, fil) for fil in filenames])

        app.run(host='0.0.0.0')
