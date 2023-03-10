import os
import pathlib
import redis as srv
from redis.commands.search.query import NumericFilter, Query

import mds_py.utils as utl
from . vocabulary import Vocabulary as voc

from . commands import Commands as cmd

class Client:  

    __instance = None

    def __new__(cls, *args, **kwargs):
        if not Client.__instance:
            Client.__instance = object.__new__(cls)
        return Client.__instance 
    
    def __init__(self, _mds_home: str|None = None ):
        if _mds_home is not None:
            if _mds_home in os.environ:
                self.mds_home = os.environ.get(_mds_home) 
            elif os.path.exists(_mds_home):
                self.mds_home = _mds_home
            else:
                self.mds_home = None
                raise RuntimeError(f"Error: Provided '{_mds_home}' mds home directory doesn't exist.")        
        elif voc.MDS_PY in os.environ:
                self.mds_home = os.environ.get(voc.MDS_PY)
        else:
            self.mds_home = None
            raise RuntimeError(f"Error: Provided '{_mds_home}' mds home directory doesn't exist.")

         # set path for all standard directories in mds_home
        self.boot = os.path.join(self.mds_home, voc.BOOTSTRAP)
        self.config = os.path.join(self.mds_home, voc.CONFIG)
        self.processors = os.path.join(self.mds_home, voc.PROCESSORS)
        self.schemas = os.path.join(self.mds_home, voc.SCHEMAS)
        self.scripts = os.path.join(self.mds_home, voc.SCRIPTS)
        self.sqlite_files = os.path.join(self.mds_home, voc.SQLITE_FILES)
        
        path = os.path.join(self.mds_home, voc.CONFIG, utl.idxFileWithExt(voc.CONFIG_FILE)) 
        self.config_props = utl.getConfig(path) 

        Client.bootstrap(self)
    
    @staticmethod
    def bootstrap(self):
        rs = utl.getRedis(self.config_props)
        '''First create idx_reg index'''
        schema_path = os.path.join(self.mds_home, voc.BOOTSTRAP, utl.idxFileWithExt(voc.IDX_REG))
        cmd.createIndex(rs, voc.IDX_REG, self.mds_home, schema_path)
        '''
        get idx files from bootstrap directory
        and register them in idx_reg index
        all including idx_rg index itself 
        '''
        fileList = utl.fileList(self.boot)
        print('File List: \n {}'.format(fileList))

        # rs: redis.Redis, mds_home:str, dir: str, fileList: list, register: bool
        cmd.createIndices(rs, self.mds_home, self.boot, fileList)

    # Following is a list  wrappers for commands from Commands module
    #====================================================================
    def schema_file_name(self, schema_dir: str, schema_name: str) -> str|None:
        return os.path.join(self.mds_home, schema_dir, utl.idxFileWithExt(schema_name))

    def schema_from_file(self, file_name: str) -> str|None:
        return utl.getSchemaFromFile(file_name)

    def index_info(self, idx_name: str) -> str|None:
        rs = utl.getRedis(self.config_props)
        return rs.ft(idx_name).info()

    def create_index(self, schema_dir: str, idx_name: str, proc: bool = False) -> str|None :
        rs = utl.getRedis(self.config_props)
        path = os.path.join(self.mds_home, schema_dir, utl.idxFileWithExt(idx_name))
        ret_str = cmd.createIndex(rs, idx_name, self.mds_home, path, proc)

        return ret_str
    
    @staticmethod
    def update_record(self, schema_dir: str, schema_name: str, map: dict) -> str|None:
        rs = utl.getRedis(self.config_props)
        path = os.path.join(self.mds_home, schema_dir, utl.idxFileWithExt(schema_name))
        # rs:redis.Redis, pref: str, idx_name: str, schema_path: str, map:dict
        # rs:redis.Redis, pref: str, schema_path: str, map:dict
        return cmd.updateRecord(rs, schema_name, path, map)

    def search(self, idx: str, query: str|Query, query_params: dict|None = None):
        rs = utl.getRedis(self.config_props)            
        return cmd.search(rs, idx, query)

    # rs: redis.Redis, index: str, query: str|Query, query_params: dict|None = None 
    def tx_lock(self, proc_id: str, query: str, batch: int = 100):
        rs = utl.getRedis(self.config_props) 
        limit = {
            'limit': batch
        }
        return cmd.search(rs, voc.TRANSACTION, query, limit)

    # rs: redis.Redis, proc_id: str, proc_pref: str, item_id: str, item_prefix: str, status: str
    def tx_status(self, proc_id: str, proc_pref: str, item_id: str, item_prefix: str, status: str) -> str|None:
        rs = utl.getRedis(self.config_props)
        return cmd.txStatus(rs, proc_id, proc_pref, item_id, status)

    '''
        dir_meta and file_meta are source processors. Normally, processor should not take care about managing "transaction" index,
        this is Controller responsibility. Source processors are exception of this rule. They populate "transaction" index to be used 
        by other processors.
        redis-mds should provide source processors for most of data sources like files, data bases (relational and non relational), 
        documents , images, video, audio and other streaming data sources.
    '''
    def dir_meta(self, proc_id: str, proc_pref: str, parent_id: str, dir: str) -> dict|None:
        rs = utl.getRedis(self.config_props)        
        map = {
            voc.PARENT_ID: f'{parent_id}',
            voc.URL: f'{dir}',
            voc.LABEL: voc.DIR.upper(),
            voc.DOC: ''
        }
        _map: dict = Client.update_record(self, schema_dir=voc.SCHEMAS, schema_name=voc.DIR, map=map) 
        if _map == None:
            return None   
        else:
            st_map: dict = cmd.txUpdate(rs, proc_id, proc_pref, _map[voc.ID], _map[voc.ITEM_PREFIX], voc.DIR, voc.WAITING)
            if st_map == None:
                return None
            else:
                return _map

    def file_meta(self, proc_id: str, proc_pref: str, dir: dict, file: str) -> dict|None:
        rs = utl.getRedis(self.config_props)
        stats = os.stat(file)
        map = {
            voc.PARENT_ID: f'{dir.get(voc.ID)}',
            voc.URL: f'{file}',
            voc.LABEL: voc.FILE.upper(),
            voc.FILE_TYPE: pathlib.Path(file).suffix,
            voc.SIZE: stats.st_size,
            voc.DOC: ''
        }
        _map: dict = Client.update_record(self, schema_dir=voc.SCHEMAS, schema_name=voc.FILE, map=map) 
        if _map == None:
            return None   
        else:
            st_map: dict = cmd.txUpdate(rs, proc_id, proc_pref, _map[voc.ID], _map[voc.ITEM_PREFIX], _map[voc.FILE_TYPE], voc.WAITING)
            if st_map == None:
                return None
            else:
                return _map

    
    print('=================== Client new instance =============================')