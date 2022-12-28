import os
from mds_py import utils as utl
from mds_py.vocabulary import Vocabulary as voc

class Controller:
    def __init__(self, path:str, data:dict):
        uri = os.path.normpath(os.path.join(path, data.get(voc.PACKAGE), data.get(voc.NAME)))
        print(uri)
        self.processor = utl.importFromURI(uri, True)
        self.data = data

    def source(self) -> dict|None:
        _data:dict = self.processor.run(self.data.get(voc.PROPS))
        return _data
