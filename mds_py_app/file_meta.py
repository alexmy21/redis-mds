import os
import sys
import mds_py.utils as utl
from mds_py.client import Client
from mds_py.vocabulary import Vocabulary as voc
from pathlib import Path

directory = "/home/alexmy/Downloads/Ontology/hackathon/demo"

client = Client()

schema = client.schema_file_name(voc.SCHEMAS, voc.FILE)
client.create_index(voc.SCHEMAS, voc.FILE)

def run(dir: dict|None):

    if dir != None:
        directory = dir.get('directory')

    for file in Path(directory).glob("**/*.csv"):
        print('HEADER: {}'.format(utl.csvHeader(file)))
        ret = client.file_meta('file_meta', 'meta', file)
        if ret == None:
            print('Error updating: {}'.format(file))

    print(os.path.dirname(__file__))

if __name__ == "__main__":
    globals()[sys.argv[1] ](sys.argv[2])
