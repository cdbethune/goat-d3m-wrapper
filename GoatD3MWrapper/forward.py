import os
import sys
import subprocess
import collections
import pandas as pd
import requests
import time
import typing
from json import JSONDecoder
from typing import List, Tuple

from d3m.primitive_interfaces.transformer import TransformerPrimitiveBase
from d3m.primitive_interfaces.base import CallResult
from d3m import container, utils
from d3m.metadata import hyperparams, base as metadata_base

from d3m.container import DataFrame as d3m_DataFrame
from d3m.container import List as d3m_List
from common_primitives import utils as utils_cp


__author__ = 'Distil'
__version__ = '1.0.5'
__contact__ = 'mailto:paul@newknowledge.io'


Inputs = container.pandas.DataFrame
Outputs = container.pandas.DataFrame

class Hyperparams(hyperparams.Hyperparams):
    target_columns = hyperparams.Set(
        elements=hyperparams.Hyperparameter[str](''),
        default=(),
        max_size=sys.maxsize,
        min_size=0,
        semantic_types=['https://metadata.datadrivendiscovery.org/types/ControlParameter'],
        description='names of columns with image paths'
    )

    rampup = hyperparams.UniformInt(lower=1, upper=sys.maxsize, default=10, semantic_types=[
        'https://metadata.datadrivendiscovery.org/types/TuningParameter'],
        description='ramp-up time, to give elastic search database time to startup, may vary based on infrastructure')


class goat(TransformerPrimitiveBase[Inputs, Outputs, Hyperparams]):
    """
        Geocode all names of locations in specified columns into lat/long pairs. 
    """
    
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_base.PrimitiveMetadata({
        # Simply an UUID generated once and fixed forever. Generated using "uuid.uuid4()".
        'id': "c7c61da3-cf57-354e-8841-664853370106",
        'version': __version__,
        'name': "Goat_forward",
        # Keywords do not have a controlled vocabulary. Authors can put here whatever they find suitable.
        'keywords': ['Geocoder'],
        'source': {
            'name': __author__,
            'contact': __contact__,
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
            "file_uri": "http://public.datadrivendiscovery.org/photon-db.tar.gz",
            "file_digest":"eaa06866b104e47116af7cb29edb4d946cbef3be701574008b3e938c32d8c020"
        }],
        # The same path the primitive is registered with entry points in setup.py.
        'python_path': 'd3m.primitives.data_cleaning.multitable_featurization.Goat_forward',
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
        self.volumes = volumes

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
        # LRU Cache helper class
        class LRUCache:
            def __init__(self, capacity):
                self.capacity = capacity
                self.cache = collections.OrderedDict()

            def get(self, key):
                try:
                    value = self.cache.pop(key)
                    self.cache[key] = value
                    return value
                except KeyError:
                    return -1

            def set(self, key, value):
                try:
                    self.cache.pop(key)
                except KeyError:
                    if len(self.cache) >= self.capacity:
                        self.cache.popitem(last=False)
                self.cache[key] = value

        goat_cache = LRUCache(10) # should length be a hyper-parameter?
        target_columns = self.hyperparams['target_columns']
        rampup = self.hyperparams['rampup']
        frame = inputs
        out_df = pd.DataFrame(index=range(frame.shape[0]),columns=target_columns)
        # the `12g` in the following may become a hyper-parameter in the future
        PopenObj = subprocess.Popen(["java","-Xms12g","-Xmx12g","-jar","photon-0.2.7.jar"],cwd=self.volumes['photon-db-latest'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        time.sleep(rampup)
        address = 'http://localhost:2322/'
        # geocode each requested location
        for i,ith_column in enumerate(target_columns):
            j = 0
            for location in frame.ix[:,ith_column]:
                cache_ret = goat_cache.get(location)
                if(cache_ret==-1):
                    r = requests.get(address+'api?q='+location)
                    tmp = self._decoder.decode(r.text)
                    if tmp['features'][0]['geometry']['coordinates']:
                        out_df.ix[j,i] = str(tmp['features'][0]['geometry']['coordinates'])
                    goat_cache.set(location,out_df.ix[j,i])
                else:
                    out_df.ix[j,i] = cache_ret
                j=j+1
        # need to cleanup by closing the server when done...
        PopenObj.kill()
        # Build d3m-type dataframe
        d3m_df = d3m_DataFrame(out_df)
        for i,ith_column in enumerate(target_columns):
            # for every column
            col_dict = dict(d3m_df.metadata.query((metadata_base.ALL_ELEMENTS, i)))
            col_dict['structural_type'] = type("it is a string")
            col_dict['name'] = target_columns[i]
            col_dict['semantic_types'] = ('https://metadata.datadrivendiscovery.org/types/FloatVector', 'https://metadata.datadrivendiscovery.org/types/Attribute')
            d3m_df.metadata = d3m_df.metadata.update((metadata_base.ALL_ELEMENTS, i), col_dict)

        return CallResult(d3m_df)

if __name__ == '__main__':
    input_df = pd.DataFrame(data={'Name':['Paul','Ben'],'Location':['Austin','New York City']})
    volumes = {} # d3m large primitive architecture dict of large files
    volumes["photon-db-latest"] = "/geocodingdata"
    from d3m.primitives.data_cleaning.multitable_featurization import Goat_forward as goat # form of import
    client = goat(hyperparams={'target_columns':['Location'],'rampup':20},volumes=volumes)
    start = time.time()
    result = client.produce(inputs = input_df)
    end = time.time()
    print("geocoding...")
    print("result:")
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)