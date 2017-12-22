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


Inputs = List[float] # container.List[float]?
Outputs = dict


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    pass


class reverse_goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams]):
    
    # make sure to populate this with JSON annotations later
    metadata = metadata_module.PrimitiveMetadata({})
    
    def __init__(self, address: str, *, hyperparams: Hyperparams, random_seed: int = 0, docker_containers: typing.Dict[str, str] = None)-> None:
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
        Accept a lat/long pair, process it and return corresponding geographic location (as GeoJSON dict,
        see geojson).
        
        Parameters
        ----------
        inputs : List of 2 coordinate float values, i.e., [longitude,latitude]

        Returns
        -------
        Outputs
            a dictionary in GeoJSON format (sub-dictionary 'features/0/properties' to be precise)
        """
            
        return self.getLocationDict(inputs)
            
    def getLocationDict(self,in_str:List[float]) -> dict:
        try:
            r = requests.get(self.address+'reverse?lon='+str(in_str[0])+'&lat='+str(in_str[1]))
            
            result = self.decoder.decode(r.text)['features'][0]['properties']
            
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = reverse_goat(address)
    in_str = list([-0.18,5.6])
    print("reverse geocoding the coordinates:")
    print(in_str)
    print("DEBUG::result (dictionary):")
    start = time.time()
    result = client.produce(in_str)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)