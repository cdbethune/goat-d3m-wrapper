import pickle
import requests
import ast
import time
from json import JSONDecoder
from typing import List, Tuple
from primitive_interfaces.base import PrimitiveBase

Inputs = str
Outputs = List[float]
Params = dict
CallMetadata = dict

class goat(PrimitiveBase[Inputs, Outputs, Params]):
    __author__ = "distil"
    __metadata__ = {}
    def __init__(self, address: str)-> None:
        self.address = address
        self.decoder = JSONDecoder()
        self.callMetadata = {}
        self.params = {}
        
    def fit(self) -> None:
        pass
    
    def get_params(self) -> Params:
        return self.params

    def set_params(self, params: Params) -> None:
        self.params = params

    def get_call_metadata(self) -> CallMetadata:
        return self.callMetadata

    def produce(self, inputs: Inputs, timeout: float = None, iterations: int = None) -> Outputs:
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
        return self.getCoordinates(Inputs)
        
            
    def getCoordinates(self,in_str:str) -> List[float]:
        try:
            print("DEBUG::starting")
            r = requests.get(self.address+'api?q='+in_str)
            
            result = self.decoder.decode(r.text)['features'][0]['geometry']['coordinates']
            print("DEBUG::done")
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = goat(address)
    in_str = '3810 medical pkwy' # addresses work! so does 'austin', etc
    start = time.time()
    result = client.getCoordinates(in_str)
    end = time.time()
    print("geocoding "+in_str)
    print("DEBUG::result ([long,lat]):")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)