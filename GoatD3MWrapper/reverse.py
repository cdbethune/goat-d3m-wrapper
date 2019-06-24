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
__version__ = '1.0.7'
__contact__ = 'mailto:nklabs@newknowledge.com'


Inputs = container.pandas.DataFrame
Outputs = container.pandas.DataFrame

class Hyperparams(hyperparams.Hyperparams):
    rampup = hyperparams.UniformInt(lower=1, upper=sys.maxsize, default=10, semantic_types=[
        'https://metadata.datadrivendiscovery.org/types/TuningParameter'],
        description='ramp-up time, to give elastic search database time to startup, may vary based on infrastructure')


class reverse_goat(TransformerPrimitiveBase[Inputs, Outputs, Hyperparams]):
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
        geocoding possible, otherwise NaNs) appended as new columns
    """
    # Make sure to populate this with JSON annotations...
    # This should contain only metadata which cannot be automatically determined from the code.
    metadata = metadata_base.PrimitiveMetadata(
        {
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
                "type": "UBUNTU",
                "package": "default-jre",
                "version": "2:1.8-56ubuntu2"
            },
            {
                "type": "TGZ",
                "key": "photon-db-latest",
                "file_uri": "http://public.datadrivendiscovery.org/photon-db.tar.gz",
                "file_digest":"eaa06866b104e47116af7cb29edb4d946cbef3be701574008b3e938c32d8c020"
            }],
            # The same path the primitive is registered with entry points in setup.py.
            'python_path': 'd3m.primitives.data_cleaning.geocoding.Goat_reverse',
            # Choose these from a controlled vocabulary in the schema. If anything is missing which would
            # best describe the primitive, make a merge request.
            'algorithm_types': [
                metadata_base.PrimitiveAlgorithmType.NUMERICAL_METHOD,
            ],
            'primitive_family': metadata_base.PrimitiveFamily.DATA_CLEANING,
        }
    )
    
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
                key = ''.join(str(e) for e in key)
                try:
                    value = self.cache.pop(key)
                    self.cache[key] = value
                    return value
                except KeyError:
                    return -1

            def set(self, key, value):
                key = ''.join(str(e) for e in key)
                try:
                    self.cache.pop(key)
                except KeyError:
                    if len(self.cache) >= self.capacity:
                        self.cache.popitem(last=False)
                self.cache[key] = value

        # find location columns, real columns, and real-vector columns
        targets = inputs.metadata.get_columns_with_semantic_type('https://metadata.datadrivendiscovery.org/types/Location')
        real_values = inputs.metadata.get_columns_with_semantic_type('http://schema.org/Float')
        real_values += inputs.metadata.get_columns_with_semantic_type('http://schema.org/Integer')
        real_values = list(set(real_values))
        real_vectors = inputs.metadata.get_columns_with_semantic_type('https://metadata.datadrivendiscovery.org/types/FloatVector')
        target_column_idxs = []
        target_columns = []

        # convert target columns to list if they have single value and are adjacent in the df
        for target, target_col in zip(targets, [list(inputs)[idx] for idx in targets]):
            if target in real_vectors:
                target_column_idxs.append(target)
                target_columns.append(target_col)
            # pair of individual lat / lon columns already in list
            elif list(inputs)[target - 1] in target_columns:
                continue
            elif target in real_values:
                if target+1 in real_values:
                    # convert to single column with list of [lat, lon]
                    col_name = "new_col_" + target_col
                    inputs[col_name] =  inputs.iloc[:,target:target+2].values.tolist()
                    target_columns.append(col_name)
                    target_column_idxs.append(target)
        print(f'target columns found: {target_columns}', file = sys.__stdout__)
        
        # make sure columns are structured as 1) lon , 2) lat pairs
        if inputs[target_columns].map(lambda x: x[1]).max() > 90:
            inputs[target_columns] = inputs[target_columns].map(lambda x: x[::-1])

        # delete columns with path names of nested media files
        outputs = inputs.remove_columns(target_column_idxs)

        goat_cache = LRUCache(10)
        out_df = pd.DataFrame(index=range(inputs.shape[0]),columns=target_columns)
        # the `12g` in the following may become a hyper-parameter in the future
        PopenObj = subprocess.Popen(["java", "-Xms12g","-Xmx12g","-jar","photon-0.3.1.jar"],cwd=self.volumes['photon-db-latest'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        print('sub-process opened\n', file = sys.__stdout__)
        time.sleep(self.hyperparams['rampup'])
        address = 'http://localhost:2322/'

        # reverse-geocode each requested location
        for i,ith_column in enumerate(target_columns):
            j = 0
            for longlat in inputs[ith_column]:
                print(f'longlat: {longlat}', file= sys.__stdout__)
                print(f'type longlat: {type(longlat)}', file= sys.__stdout__)
                print(f'longlat 0: {longlat[0]}', file= sys.__stdout__)
                cache_ret = goat_cache.get(longlat)
                if(cache_ret==-1):
                    r = requests.get(address+'reverse?lon='+str(longlat[0])+'&lat='+str(longlat[1]))
                    print('request successfully made!',  file = sys.__stdout__)
                    tmp = self._decoder.decode(r.text)
                    if tmp['features'][0]['properties']['name']:
                        out_df.iloc[j,i] = tmp['features'][0]['properties']['name']
                    goat_cache.set(longlat,out_df.iloc[j,i])
                else:
                    out_df.iloc[j,i] = cache_ret
                j=j+1
        # need to cleanup by closing the server when done...
        PopenObj.kill()
        print(out_df.head(), file = sys.__stdout__)
        print(outputs.head(), file = sys.__stdout__)
        # Build d3m-type dataframe
        d3m_df = d3m_DataFrame(out_df)
        for i,ith_column in enumerate(target_columns):
            # for every column
            col_dict = dict(d3m_df.metadata.query((metadata_base.ALL_ELEMENTS, i)))
            col_dict['structural_type'] = type("it is a string")
            col_dict['name'] = target_columns[i]
            col_dict['semantic_types'] = ('http://schema.org/Text', 'https://metadata.datadrivendiscovery.org/types/Attribute')
            d3m_df.metadata = d3m_df.metadata.update((metadata_base.ALL_ELEMENTS, i), col_dict)
        df_dict = dict(d3m_df.metadata.query((metadata_base.ALL_ELEMENTS, )))
        df_dict_1 = dict(d3m_df.metadata.query((metadata_base.ALL_ELEMENTS, ))) 
        df_dict['dimension'] = df_dict_1
        df_dict_1['name'] = 'columns'
        df_dict_1['semantic_types'] = ('https://metadata.datadrivendiscovery.org/types/TabularColumn',)
        df_dict_1['length'] = d3m_df.shape[1]
        d3m_df.metadata = d3m_df.metadata.update((metadata_base.ALL_ELEMENTS,), df_dict)
        return CallResult(outputs.append_columns(d3m_df))
    
if __name__ == '__main__':
    input_df = pd.DataFrame(data={'Name':['Paul','Ben'],'Long/Lat':[list([-97.7436995, 30.2711286]),list([-73.9866136, 40.7306458])]})
    volumes = {} # d3m large primitive architecture dict of large files
    volumes["photon-db-latest"] = "/geocodingdata"
    from d3m.primitives.data_cleaning.multitable_featurization import Goat_reverse as reverse_goat # form of import
    client = reverse_goat(hyperparams={'target_columns':['Long/Lat'],'rampup':8},volumes=volumes)
    print("reverse geocoding...")
    print("result:")
    start = time.time()
    result = client.produce(inputs = input_df)
    end = time.time()
    print(result)
    print("time elapsed is (in sec):")
    print(end-start)
