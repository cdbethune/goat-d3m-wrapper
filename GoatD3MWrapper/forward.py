import os
import sys
import subprocess
import pandas as pd
import requests
import time
import typing
from json import JSONDecoder
from typing import List, Tuple

from d3m.primitive_interfaces.base import PrimitiveBase, CallResult
from d3m import container, utils
from d3m.metadata import hyperparams, base as metadata_base, params

__author__ = 'Distil'
__version__ = '1.0.2'


Inputs = container.pandas.DataFrame
Outputs = container.pandas.DataFrame


class Params(params.Params):
    pass


class Hyperparams(hyperparams.Hyperparams):
    target_columns = hyperparams.Set(
        elements=hyperparams.Hyperparameter[str](''),
        default=(),
        max_size=sys.maxsize,
        min_size=0,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='names of columns with image paths'
    ),
    rampup = hyperparams.UniformInt(lower=1, upper=sys.maxsize, default=10, semantic_types=[
        'https://metadata.datadrivendiscovery.org/types/TuningParameter'],
        description='ramp-up time, to give elastic search database time to startup, may vary based on infrastructure')


class goat(PrimitiveBase[Inputs, Outputs, Params, Hyperparams]):
    
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_base.PrimitiveMetadata({
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
        },
        {
            "type": "TGZ",
            "key": "photon-db-latest",
            "file_uri": "http://public.datadrivendiscovery.org/photon-db-latest.tar.gz",
            "file_digest":"7690aa52af17d63bf1380862fbc5270043ffea264a04f1e1ac4a590229cc615a"
        }],
        # The same path the primitive is registered with entry points in setup.py.
        'python_path': 'd3m.primitives.distil.Goat.forward',
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
        Accept a set of location strings, processes it and returns a set of long/lat coordinates.
        
        Parameters
        ----------
        inputs : pandas dataframe containing strings representing some geographic locations -
                 (name, address, etc) - one location per row in the specified target column
        
        timeout : float
            A maximum time this primitive should take to produce outputs during this method call, in seconds.
            Inapplicable for now...
        iterations : int
            How many of internal iterations should the primitive do. Inapplicable for now...

        Returns
        -------
        Outputs
            Pandas dataframe, with a List of 2 floats [longitude, latitude] per row/location
        """
        target_columns = self.hyperparams['target_columns']
        rampup = self.hyperparams['rampup']
        frame = inputs
        # for now, just one target column is handled
        out_df = pd.DataFrame(index=range(frame.shape[0]),columns=target_columns)
        
        PopenObj = subprocess.Popen(["java","-jar","photon-0.2.7.jar"],cwd=self.volumes['photon-db-latest'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        time.sleep(rampup)
        address = 'http://localhost:2322/'
        # geocode each requested location
        for i,ith_column in enumerate(target_columns):
            j = 0
            for location in frame.ix[:,ith_column]:
                r = requests.get(address+'api?q='+location)
                tmp = self._decoder.decode(r.text)
                if tmp['features']:
                    out_df.ix[j,i] = tmp['features'][0]['geometry']['coordinates']
                j=j+1
        # need to cleanup by closing the server when done...
        PopenObj.kill()

        # print("DEBUG::out_df")
        # print(out_df)

        return CallResult(out_df)

if __name__ == '__main__':
    input_df = pd.DataFrame(data={'Name':['Paul','Ben'],'Location':['Austin','New York City']})
    volumes = {} # d3m large primitive architecture dict of large files
    volumes["photon-db-latest"] = "/geocodingdata"
    from d3m.primitives.distil.Goat import forward as goat # form of import
    client = goat(hyperparams={'target_columns':['Location'],'rampup':8},volumes=volumes)
    start = time.time()
    result = client.produce(inputs = input_df)
    end = time.time()
    print("geocoding...")
    print("result:")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)