
import csv
from . vocabulary import Vocabulary as voc

import importlib
import os

import os
import yaml
import redis
import hashlib

from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.query import NumericFilter, Query


doc_0 = {'props': {}}

def importFromURI(uri, absl):
    mod = None
    if not absl:
        print(os.path.dirname(__file__))
        uri = os.path.normpath(os.path.join(os.path.dirname(__file__), uri))
    path, fname = os.path.split(uri)
    mname, ext = os.path.splitext(fname)

    if os.path.exists(os.path.join(path,mname)+'.pyc'):
        try:
            return imp.load_compiled(mname, uri)
        except:
            pass
    if os.path.exists(os.path.join(path,mname)+'.py'):
        try:
            return imp.load_source(mname, uri)
        except:
            pass

    return mod

def loadClassFromFile(filepath: str, expected_class: str = None):
    class_inst = None
    #  expected_class = 'MyClass'

    mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])

    if file_ext.lower() == '.py':
        py_mod = imp.load_source(mod_name, filepath)

    elif file_ext.lower() == '.pyc':
        py_mod = imp.load_compiled(mod_name, filepath)

    if hasattr(py_mod, expected_class):
        class_inst = getattr(py_mod, expected_class)()

    return class_inst

def fileList(dir: str) -> list|None:
    return os.listdir(dir)

def getSchemaFromFile(file_name):     
    with open(file_name, 'r') as file:
        return yaml.safe_load(file)

def schema_name(file_name: str) -> str|None:
    return file_name.split('.')[0]

def idxFileWithExt(schema: str) -> str|None:
    if schema.endswith('.yaml'):
        return
    else:
        return schema + '.yaml' 

def getConfig(file_name: str|None) -> dict|None:
    config: dict | None
    try:
        with open(file_name, 'r') as file:
            config = yaml.safe_load(file)
    except:
        raise RuntimeError(f"Error: Problem with '{file_name}' file.")            
    return config
   
def getRedis(config: dict) -> redis.Redis|None:
    host = config.get(voc.REDIS, {}).get(voc.REDIS_HOST)
    port = config.get(voc.REDIS, {}).get(voc.REDIS_PORT)

    return redis.StrictRedis(host, port)      

def ft_schema(schema: dict) -> tuple|None:
    dictlist = []
    tmp: str
    for key, value in schema:
        if value == 'tag':
            temp = TagField(key)
            dictlist.append(temp)
        elif value == 'numeric':
            temp = NumericField(key)
            dictlist.append(temp)
        else:
            temp = TextField(key)
            dictlist.append(temp) 
    return tuple(i for i in dictlist)

# Generates SHA1 hash code from key fields of props 
# dictionary
def sha1(keys: list, props: dict) -> str|None:
    sha = '' 
    for key in keys:
        # Normalize strings by turing to low case
        # and removing spaces
        if props.get(str(key)) != None:
            sha+= props.get(str(key).lower().replace(' ', ''))

    m = hashlib.sha1()
    m.update(sha.encode())
    return m.hexdigest()

'''
    Mics methods
'''

def prefix(term: str) -> str:
    if term.endswith(':'):
        return term
    else:
        return term + ':'

def underScore(term: str) -> str|None:
    if term.startswith('_'):
        return term
    else:
        return '_' + term

def csvHeader(file_path: str) -> bool|None:
    try:
        with open(file_path, mode = 'r', encoding="utf-8", errors="backslashreplace") as csvfile:
            return csv.Sniffer().has_header(csvfile.read(4096))
    except:
        print('Error reading file')
        return None