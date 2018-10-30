import os
import subprocess
import requests
import time
import typing
from json import JSONDecoder
from typing import List, Tuple

from d3m.primitive_interfaces.base import PrimitiveBase, CallResult
from d3m import container, utils
from d3m.metadata import hyperparams, base as metadata_base, params

__author__ = 'Distil'
__version__ = '1.0.1'


Inputs = container.List #container.pandas.DataFrame
Outputs = container.List #container.pandas.DataFrame


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    pass


class reverse_goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams]):
    
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_base.PrimitiveMetadata({
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
                "https://github.com/NewKnowledge/goat-d3m-wrapper",
            ],
        },
        # A list of dependencies in order. These can be Python packages, system packages, or Docker images.
        # Of course Python packages can also have their own dependencies, but sometimes it is necessary to
        # install a Python package first to be even able to run setup.py of another package. Or you have
        # a dependency which is not on PyPi.
         'installation': [{
            'type': metadata_base.PrimitiveInstallationType.PIP,
            'package_uri': 'git+https://github.com/NewKnowledge/goat-d3m-wrapper.git@{git_commit}#egg=GoatD3MWrapper'.format(
                git_commit=utils.current_git_commit(os.path.dirname(__file__)),
            ),
        }],
        # The same path the primitive is registered with entry points in setup.py.
        'python_path': 'd3m.primitives.distil.Goat.reverse',
        # Choose these from a controlled vocabulary in the schema. If anything is missing which would
        # best describe the primitive, make a merge request.
        'algorithm_types': [
            metadata_base.PrimitiveAlgorithmType.NUMERICAL_METHOD,
        ],
        'primitive_family': metadata_base.PrimitiveFamily.DATA_CLEANING,
    })
    
    def __init__(self, *, hyperparams: Hyperparams, random_seed: int = 0, volumes: typing.Dict[str, str] = None)-> None:
        super().__init__(hyperparams=hyperparams, random_seed=random_seed, volumes=volumes)        
        
        self._decoder = JSONDecoder()
        self._params = {}

        self.volumes = volumes
        
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
            
        # try:
        PopenObj = subprocess.Popen(["java","-jar","photon-0.2.7.jar"],cwd=self.volumes['photon-db-latest'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        time.sleep(10)
        address = 'http://localhost:2322/'
        print("before requests.get")
        r = requests.get(address+'reverse?lon='+inputs[0]+'&lat='+inputs[1])
        print("after...")
        # need to cleanup by closing the server when done...
        PopenObj.kill()
        # return the top result at that location!!
        result=[]
        tmp = self._decoder.decode(r.text)
        if tmp:
            result = tmp['features'][0]['properties']
            
        return result
            
        # except:
            # Should probably do some more sophisticated error logging here
            # return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    volumes = {} # d3m large primitive architecture dict of large files
    volumes["photon-db-latest"] = "/geocodingdata/"
    from d3m.primitives.distil.Goat import reverse as reverse_goat # form of import
    client = reverse_goat(hyperparams={},volumes=volumes)
    in_str = list(["-97.59","30.35"])
    print("reverse geocoding the coordinates:")
    print(in_str)
    print("result (dictionary list of size 1):")
    start = time.time()
    result = client.produce(inputs = in_str)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)