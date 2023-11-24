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
import lipsync_schema_pb2
import numpy as np
def workload(received_data):
    request = lipsync_schema_pb2.RequestData()
    request.ParseFromString(received_data) #231546
    face = np.array(request.face).reshape([1,96,96,6])
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

from server_ml_echo import ML_socket_server
class My_ML_socket_server(ML_socket_server):
    def workload(self, data):
        return workload(data)
    

if __name__ == "__main__":
    
    my_ML_socket_server = My_ML_socket_server()
    my_ML_socket_server.start()
