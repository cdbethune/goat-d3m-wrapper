import pickle
import requests
import ast
import time
import typing
from json import JSONDecoder
from typing import List, Tuple
from primitive_interfaces.base import PrimitiveBase, CallResult

from d3m_metadata import container, hyperparams, metadata as metadata_module, params, utils

__author__ = 'Distil'
__version__ = '1.0.0'


Inputs = container.List[str]
Outputs = container.List[float] # container.List[float]?


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    pass


class goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams],address:str):
    
    # make sure to populate this with JSON annotations later
    metadata = metadata_module.PrimitiveMetadata({})
    
    def __init__(self, *, hyperparams: Hyperparams, random_seed: int = 0, docker_containers: typing.Dict[str, str] = None, address:str)-> None:
        super().__init__(hyperparams=hyperparams, random_seed=random_seed, docker_containers=docker_containers)
                
        self.address = address
        self.decoder = JSONDecoder()
        self.params = {}
        
    def fit(self) -> None:
        pass
    
    def get_params(self) -> Params:
        return self.params

    def set_params(self, *, params: Params) -> None:
        self.params = params
    
    def set_training_data(self, *, inputs: Inputs, outputs: Outputs) -> None:
        pass

    def produce(self, *, inputs: Inputs, timeout: float = None, iterations: int = None) -> CallResult[Outputs]:
        """
        Accept a location string, process it and return long/lat coordinates.
        
        Parameters
        ----------
        inputs : string representing some geographic location (name, address, etc)
        
        timeout : float
            A maximum time this primitive should take to produce outputs during this method call, in seconds.
            Inapplicable for now...
        iterations : int
            How many of internal iterations should the primitive do. Inapplicable for now...

        Returns
        -------
        Outputs
            A list of 2 floats, [longitude, latitude]
        """
        
        try:
            r = requests.get(self.address+'api?q='+inputs[0])
            
            result = self.decoder.decode(r.text)['features'][0]['geometry']['coordinates']
            
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = goat(address=address)
    in_str = '3810 medical pkwy, austin, tx' # addresses work! so does 'austin', etc
    start = time.time()
    result = client.produce(in_str)
    end = time.time()
    print("geocoding "+in_str)
    print("DEBUG::result ([long,lat]):")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)