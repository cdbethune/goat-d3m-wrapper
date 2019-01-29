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
from d3m.metadata import hyperparams, base as metadata_base, params

from d3m.container import DataFrame as d3m_DataFrame
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
    ),
    rampup = hyperparams.UniformInt(lower=1, upper=sys.maxsize, default=10, semantic_types=[
        'https://metadata.datadrivendiscovery.org/types/TuningParameter'],
        description='ramp-up time, to give elastic search database time to startup, may vary based on infrastructure')


class reverse_goat(TransformerPrimitiveBase[Inputs, Outputs, Hyperparams]):
    """
        Reverse geocode lat/long pairs in specified columns into names of locations. 
    """
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_base.PrimitiveMetadata({
        # Simply an UUID generated once and fixed forever. Generated using "uuid.uuid4()".
        'id': "f6e4880b-98c7-32f0-b687-a4b1d74c8f99",
        'version': __version__,
        'name': "Goat_reverse",
        # Keywords do not have a controlled vocabulary. Authors can put here whatever they find suitable.
        'keywords': ['Reverse Geocoder'],
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
        'python_path': 'd3m.primitives.data_cleaning.multitable_featurization.Goat_reverse',
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
        Accept a set of lat/long pair, processes it and returns a set corresponding geographic location names
        
        Parameters
        ----------
        inputs : pandas dataframe containing 2 coordinate float values, i.e., [longitude,latitude] 
                 representing each geographic location of interest - a pair of values
                 per location/row in the specified target column

        Returns
        -------
        Outputs
            Pandas dataframe containing one location per longitude/latitude pair (if reverse
            geocoding possible, otherwise NaNs)
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
        # reverse-geocode each requested location
        for i,ith_column in enumerate(target_columns):
            j = 0
            for longlat in frame.ix[:,ith_column]:
                cache_ret = goat_cache.get(longlat)
                if(cache_ret==-1):
                    r = requests.get(address+'reverse?lon='+str(longlat[0])+'&lat='+str(longlat[1]))
                    tmp = self._decoder.decode(r.text)
                    if tmp['features'][0]['properties']['name']:
                        out_df.ix[j,i] = tmp['features'][0]['properties']['name']
                    goat_cache.set(longlat,out_df.ix[j,i])
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
            col_dict['semantic_types'] = ('http://schema.org/Text', 'https://metadata.datadrivendiscovery.org/types/Attribute')
            d3m_df.metadata = d3m_df.metadata.update((metadata_base.ALL_ELEMENTS, i), col_dict)

        return CallResult(d3m_df)
    
if __name__ == '__main__':
    input_df = pd.DataFrame(data={'Name':['Paul','Ben'],'Long/Lat':[list([-97.7436995, 30.2711286]),list([-73.9866136, 40.7306458])]})
    volumes = {} # d3m large primitive architecture dict of large files
    volumes["photon-db-latest"] = "/geocodingdata"
    from d3m.primitives.distil.Goat import reverse as reverse_goat # form of import
    client = reverse_goat(hyperparams={'target_columns':['Long/Lat'],'rampup':8},volumes=volumes)
    print("reverse geocoding...")
    print("result:")
    start = time.time()
    result = client.produce(inputs = input_df)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)