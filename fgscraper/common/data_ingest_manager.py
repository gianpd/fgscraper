import os
import json
import pathlib

from datetime import datetime, timedelta

from functools import lru_cache

from wasabi import msg

from typing import Union, List, Dict, Optional 

ROOT_PATH = pathlib.Path(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))
DATA_PATH = pathlib.Path(ROOT_PATH)/'data'

class DataIngestManager:
    
    def __init__(self, raw_enterprise_path: str):
        self._raw_enterprise_path = raw_enterprise_path
        self._root_path = ROOT_PATH
        self._data_path = DATA_PATH
    
    def write_json(self, full_dict: Dict[str, str], dir_path: Union[str, pathlib.Path], fname: str) -> None:
        dir_path = pathlib.Path(dir_path)
        if not dir_path.is_dir():
             raise ValueError(f'dir path {dir_path} is not a regular directory.')
             
        fname = pathlib.Path(fname).stem + '.json'
        abs_file_path = dir_path/fname
        msg.info(f'Writing json to {abs_file_path.name}')
        with open(abs_file_path, '+w') as raw_f:
            json.dump(full_dict, raw_f)

    def read_json(self, abs_file_path: str) -> Dict:
        if not pathlib.Path(abs_file_path).is_file():
            raise ValueError(f'{abs_file_path} is not a regular file.')
        with open(abs_file_path) as f:
            d = json.load(f)
        return d

    def get_file_paths(self, data_path: Union[str, pathlib.Path], file_prefix: str, sort: Optional[bool] = True) -> List[str]:
        """
        Util for getting a sorted/unsorted list of file paths from disk. 

        --Parameters
         - data_path: (str, or Path), the directory path
         - file_prefix: (str), the file prefixs which should be searched (txt, json, csv, etc ..)
         - sort: (bool), if or not returning a sorted list

        --Return
         - A list containing the founded file with prefix file_prefix.

        """

        if isinstance(data_path, str):
            data_path = pathlib.Path(data_path)

        if not data_path.is_dir():
            raise ValueError(f'data path {data_path} is not a regular directory.')
        
        file_prefix = file_prefix if not file_prefix[0] == '.' else file_prefix[1:]
        ls = list(map(lambda x: x, data_path.glob(f'*.{file_prefix}')))
        msg.info(f'Founded {len(ls)} file with prefix {file_prefix} at {data_path}')
        if sort:
            return sorted(ls)
        return ls

    @lru_cache
    def get_raw_enterprise_paths(self):
        enterprise_ids_paths = self.get_file_paths(data_path=self._data_path/'raw_enterprise', file_prefix='json')
        ent_ids = list(map(lambda x: str(x.stem).split('_')[3], enterprise_ids_paths)) # list(map(lambda)) does not guarantee to return sorted elements
        return sorted(ent_ids)


    def check_if_must_run_id_and_post(self):
        id_txt = self.get_file_paths(self._data_path/'enterprise_ids', 'txt')
        if len(id_txt) == 0:
            return True
            
        ts = id_txt[0].name.split('_')[0]
        ts = datetime.strptime(ts, '%Y%m%d%H%M%S')
        elapsed = datetime.now() - ts
        if len(id_txt) != 107:
            # there are less than 107 provinces -> make all 107 posts requests
            return True
        else:
            if elapsed > timedelta(days=30):
                # the enterprise ids are too old.
                return True
        # there are 107 provinces and the collected time is not more old than a month 
        return None

    def check_if_regionIds(self):
        regionIds = self.get_file_paths(data_path=self._root_path/'assets', file_prefix='json')
        if len(regionIds) == 2:
            return True
        return None


    # def check_raw_enterprise_exist(self, enterprise_id: str):
    #     ent_ids = self.get_raw_enterprise_paths()
    #     return self.binary_search(array=ent_ids, item=enterprise_id)

    # def binary_search(self, array, item):
    #     # O(log n)
    #     left = 0
    #     right = len(array) -1
    #     if item < array[left] or item > array[right]:
    #         # element is not present
    #         return None
    #     while left <= right:
    #         mid = left + (right - left) // 2
    #         el = array[mid]
    #         if el == item:
    #             return True
    #         if el < item:
    #             left = mid + 1
    #         else:
    #             right = mid - 1
    #     return None




