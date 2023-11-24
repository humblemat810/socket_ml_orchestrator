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
logger.debug('start loading module server_ml_echo_worker.py')


from task_worker import base_socket_worker
import lipsync_schema_pb2
import numpy as np
import logging
from models import Wav2Lip
import torch
import os
logging.info("loading wav2lip model")
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print('Using {} for inference.'.format(device))
def load_model(path):
    model = Wav2Lip()
    print("Load checkpoint from: {}".format(path))
    checkpoint = _load(path)
    s = checkpoint["state_dict"]
    new_s = {}
    for k, v in s.items():
        new_s[k.replace('module.', '')] = v
    model.load_state_dict(new_s)

    model = model.to(device)
    return model.eval()
def _load(checkpoint_path):
    if device == 'cuda':
        checkpoint = torch.load(checkpoint_path)
    else:
        checkpoint = torch.load(checkpoint_path,
                                map_location=lambda storage, loc: storage)
    return checkpoint

global model
preload_model_at_start = True
if preload_model_at_start:
    model = load_model(os.path.join(os.getcwd(), "checkpoints/wav2lip.pth"))
else:
    model = None
logging.info("loaded wav2lip detection model")
class ml_echo_worker(base_socket_worker):
    def workload(self, received_data):
        request = lipsync_schema_pb2.RequestData()
        request.ParseFromString(received_data) #231546
        face = np.array(request.face).reshape([1,96,96,3])
        face_masked = face.copy()
        face[:, face.shape[2]//2:] = 0

        face = np.concatenate((face_masked, face), axis=3) / 255.
        mel = np.array(request.mel).reshape([1,1,80,16])
        face = torch.FloatTensor(np.transpose(face, (0, 3, 1, 2))).to(device)
        mel = np.array(request.mel).reshape([1,1,80,16])
        mel = torch.FloatTensor(mel).to(device)
        with torch.no_grad():
            pred = model(mel, face )
        
        face_result = pred.cpu().numpy().transpose(0, 2, 3, 1) * 255.
        response = lipsync_schema_pb2.ResponseData()
        response.messageuuid = request.messageuuid
        response.face.extend(face_result.flatten())
        serialized_data = response.SerializeToString()
        return serialized_data

print("Port number:", port)
if __name__ == "__main__":
    
    my_ML_socket_server = ml_echo_worker(server_address = ('localhost', port))
    my_ML_socket_server.start()
