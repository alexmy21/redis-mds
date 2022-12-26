import os
import sys
import mds_py.utils as utl
from mds_py.client import Client
from mds_py.vocabulary import Vocabulary as voc
from pathlib import Path

directory = "/home/alexmy/Downloads/Ontology/hackathon/demo"

client = Client()

client.create_index(voc.SCHEMAS, voc.DIR)
client.create_index(voc.SCHEMAS, voc.FILE)

def run(dir: dict|None):

    if dir != None:
        directory = dir.get('dir')

    id = client.dir_meta('dir_meta', 'meta', '', directory)

    for file in Path(directory).glob("**/*.csv"):
        # print('HEADER: {}'.format(utl.csvHeader(file)))
        ret = client.file_meta('file_meta', 'meta', id, file)
        if ret == None:
            print('Error updating: {}'.format(file))

    print(os.path.dirname(__file__))

if __name__ == "__main__":
    if sys.argv.count == 2:
        globals()[sys.argv[1] ](sys.argv[2])
    else:
        print('Requeres 2 arguments.')
