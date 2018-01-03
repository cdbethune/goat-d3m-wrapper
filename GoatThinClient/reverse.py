import os.path
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
Outputs = container.List[dict]


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    pass


class reverse_goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams]):
    
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_module.PrimitiveMetadata({
        # Simply an UUID generated once and fixed forever. Generated using "uuid.uuid4()".
        'id': "f6e4880b-98c7-32f0-b687-a4b1d74c8f99",
        'version': __version__,
        'name': "Goat.reverse",
        # Keywords do not have a controlled vocabulary. Authors can put here whatever they find suitable.
        'keywords': ['Reverse Geocoder'],
        'source': {
            'name': __author__,
            'uris': [
                # Unstructured URIs.
                "https://github.com/NewKnowledge/geocoding-thin-client",
            ],
        },
        # A list of dependencies in order. These can be Python packages, system packages, or Docker images.
        # Of course Python packages can also have their own dependencies, but sometimes it is necessary to
        # install a Python package first to be even able to run setup.py of another package. Or you have
        # a dependency which is not on PyPi.
         'installation': [{
            'type': metadata_module.PrimitiveInstallationType.PIP,
            'package_uri': 'git+https://github.com/NewKnowledge/geocoding-thin-client.git@4b0738c39a3df42633d93b56a52bde1be67745c3',
        }],
        # The same path the primitive is registered with entry points in setup.py.
        'python_path': 'd3m.primitives.distil.Goat.reverse',
        # Choose these from a controlled vocabulary in the schema. If anything is missing which would
        # best describe the primitive, make a merge request.
        'algorithm_types': [
            metadata_module.PrimitiveAlgorithmType.NUMERICAL_METHOD,
        ],
        'primitive_family': metadata_module.PrimitiveFamily.DATA_CLEANING,
    })
    
    def __init__(self, address: str, *, hyperparams: Hyperparams, random_seed: int = 0, docker_containers: typing.Dict[str, str] = None)-> None:
        super().__init__(hyperparams=hyperparams, random_seed=random_seed, docker_containers=docker_containers)        
        
        self._decoder = JSONDecoder()
        self._params = {}
        
    def fit(self) -> None:
        pass
    
    def get_params(self) -> Params:
        return self._params

    def set_params(self, *, params: Params) -> None:
        self._params = params
        
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
            
        try:
            r = requests.get(inputs[0]+'reverse?lon='+inputs[1]+'&lat='+inputs[2])
            
            result = self.decoder.decode(r.text)['features'][0]['properties']
            
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = reverse_goat()
    in_str = list([address,"-0.18","5.6"])
    print("reverse geocoding the coordinates:")
    print(in_str)
    print("DEBUG::result (dictionary list of size 1):")
    start = time.time()
    result = client.produce(in_str)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)