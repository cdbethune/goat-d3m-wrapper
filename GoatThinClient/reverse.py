import pickle
import requests
import ast
import time
from json import JSONDecoder
from typing import List

Inputs = List[float]
Outputs = dict
Params = dict
CallMetadata = dict

class reverse_goat(PrimitiveBase[Inputs, Outputs, Params]):
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
        
    def produce(self, *, inputs: Inputs, timeout: float = None, iterations: int = None) -> Outputs:
        """
        Accept a lat/long pair, process it and return corresponding geographic location (as GeoJSON dict,
        see geojson).
        
        Parameters
        ----------
        inputs : List of 2 coordinate float values, i.e., [longitude,latitude]
        
        timeout : float
            A maximum time this primitive should take to produce outputs during this method call, in seconds.
            Inapplicable for now...
        iterations : int
            How many of internal iterations should the primitive do. Inapplicable for now...

        Returns
        -------
        Outputs
            a dictionary in GeoJSON format (sub-dictionary 'features/0/properties' to be precise)
        """        
        try:
            print("DEBUG::starting")
            print("DEBUG:: address+'reverse?lon='+in_str[0]+'&lat='+in_str[1]:")
            print(self.address+'reverse?lon='+str(in_str[0])+'&lat='+str(in_str[1]))
            
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