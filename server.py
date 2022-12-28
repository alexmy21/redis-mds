import os
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from mds_py import utils as utl
from mds_py.vocabulary import Vocabulary as voc
from mds_py.controller import Controller as controller

async def post_method(request):
    data: dict = await request.json() 

    match data.get(voc.LABEL):
        case 'SOURCE':
            path = os.path.dirname(__file__)
            _data: dict = controller(path, data).source()
        case _:
            print('default case')

    return JSONResponse(_data)

def startup():
    print('Starlette started')

routes = [
    Route('/post', post_method, methods=['POST']),
]

app = Starlette(debug=True, routes=routes, on_startup=[startup])