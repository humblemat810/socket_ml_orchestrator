import logging
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description='Example program to accept a port number.')

# Add the port argument
parser.add_argument('--port', type=int, help='Port number', dest = 'port')

# Parse the command-line arguments
args = parser.parse_args()

# Access the port number
port = args.port
import os
os.environ['port'] = str(port)
# Create a logger
logger = logging.getLogger(f"server_ml_echo_worker-{port}")
logger.setLevel(logging.DEBUG)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler(f'server_ml_echo_worker{port}.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug('start loading module')


from task_worker import base_socket_worker
import lipsync_schema_pb2
import numpy as np
import logging

import torch
import os
logging.info("loading wav2lip model")
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print('Using {} for inference.'.format(device))
def load_model(path):
    # to do
    model = lambda x: x # implemnet actual model here instead of returning raw data
    return model


global model
preload_model_at_start = True
if preload_model_at_start:
    model = load_model(os.path.join(os.getcwd(), "checkpoints/{mymodel.pth.tf.caffe.weights}"))
else:
    model = None
logging.info("loaded model")
class ml_echo_worker(base_socket_worker):
    def workload(self, received_data):
        request = lipsync_schema_pb2.RequestData()
        request.ParseFromString(received_data) #231546
        face = np.array(request.face).reshape([1,96,96,3])
        # this example 

        response = lipsync_schema_pb2.ResponseData()
        response.messageuuid = request.messageuuid
        response.face.extend(face.flatten())
        serialized_data = response.SerializeToString()
        return serialized_data

print("Port number:", port)
if __name__ == "__main__":
    
    my_ML_socket_server = ml_echo_worker(server_address = ('localhost', port))
    my_ML_socket_server.start()
