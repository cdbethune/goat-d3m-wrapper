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
Outputs = container.List[float] # container.List[float]?


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    pass


class goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams]):
    
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    print("DEBUG::")
    print(__file__)
    print(utils.current_git_commit(os.path.dirname(__file__))
    metadata = metadata_module.PrimitiveMetadata({
        # Simply an UUID generated once and fixed forever. Generated using "uuid.uuid4()".
        'id': "c7c61da3-cf57-354e-8841-664853370106",
        'version': __version__,
        'name': "Goat.forward",
        # Keywords do not have a controlled vocabulary. Authors can put here whatever they find suitable.
        'keywords': ['Geocoder'],
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
            'package_uri': 'git+git://gitlab.com/datadrivendiscovery/tests-data.git@{git_commit}#subdirectory=primitives'.format(
                git_commit=utils.current_git_commit(os.path.dirname(__file__)),
            ),
        }],
        # The same path the primitive is registered with entry points in setup.py.
        'python_path': 'd3m.primitives.distil.Goat.forward',
        # Choose these from a controlled vocabulary in the schema. If anything is missing which would
        # best describe the primitive, make a merge request.
        'algorithm_types': [
            metadata_module.PrimitiveAlgorithmType.CONVOLUTIONAL_NEURAL_NETWORK,
        ],
        'primitive_family': metadata_module.PrimitiveFamily.DATA_CLEANING,
    })

    
    def __init__(self, *, hyperparams: Hyperparams, random_seed: int = 0, docker_containers: typing.Dict[str, str] = None)-> None:
        super().__init__(hyperparams=hyperparams, random_seed=random_seed, docker_containers=docker_containers)
                
        self._decoder = JSONDecoder()
        self._params = {}
        
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
            r = requests.get(inputs[0]+'api?q='+inputs[1])
            
            result = self._decoder.decode(r.text)['features'][0]['geometry']['coordinates']
            
            return result
            
        except:
            # Should probably do some more sophisticated error logging here
            return "Failed GET request to photon server, please try again..."

if __name__ == '__main__':
    address = 'http://localhost:2322/'
    client = goat()
    in_str = '3810 medical pkwy, austin, tx' # addresses work! so does 'austin', etc
    start = time.time()
    result = client.produce(list[address,in_str])
    end = time.time()
    print("geocoding "+in_str)
    print("DEBUG::result ([long,lat]):")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)