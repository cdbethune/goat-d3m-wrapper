import pickle
import requests
import ast
import time
from json import JSONDecoder
from typing import List

class geocoder:
    def __init__(self, address: str)-> None:
        self.address = address
        self.decoder = JSONDecoder()

    def process(self, in_str: str) -> List[float]:
        """ Accept a location string, process it and return long/lat coordinates...
        str: string of some geographic location (name, address, etc)
        -> a list of 2 floats, [longitude, latitude]
        """
        try:
            print("DEBUG:: address +'api?q=' + input string:")
            print(self.address+'api?q='+in_str)
            
            r = requests.get(self.address+'api?q='+in_str)
            
            result = self.decoder.decode(r.text)['features'][0]['geometry']['coordinates']
            
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, check to see that it is running..."
            
    def reverse_process(self, in_str: List[str]) -> List[str]:
        """ Accept a lat/long pair, process it and return corresponding geographic location...
        str: List of 2 coordinates, i.e., [longitude,latitude]
        -> a dictionary in geoJSON format (sub-dictionary 'features/0/properties' to be precise)...
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
            return "Failed GET request to photon server, check to see that it is running..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = geocoder(address)
    in_str = '12610 riata trace pkwy' # addresses work! so does 'austin', etc
    start = time.time()
    result = client.process(in_str)
    end = time.time()
    print("geocoding "+in_str)
    print("DEBUG::result ([long,lat]):")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)
    in_str = list([-0.18,5.6])
    print("reverse geocoding")
    print(in_str)
    print("DEBUG::result (dictionary):")
    start = time.time()
    result = client.reverse_process(in_str)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)