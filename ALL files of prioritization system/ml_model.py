import os
import pickle

class Model():

    def __init__(self):
        #LOAD ML MODEL, PREVIOUSLY TRAINED -ONE TIME
        path = os.path.join(*['static','model','finalized_model.sav'])
        self.MODEL_RF = pickle.load(open(path, 'rb'))

    def get_model(self):
        return self.MODEL_RF
