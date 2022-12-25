import os
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from starlette.routing import Route

from mds_py import utils as utl
from mds_py.vocabulary import Vocabulary as voc


async def post_method(request):
    data: dict = await request.json() 

    uri = os.path.normpath(os.path.join(os.path.dirname(__file__), data.get(voc.PROCESSOR_ID)))
    processor = utl.importFromURI(uri, True)
    processor.run(data.get(voc.PROPS))

    return JSONResponse(data.get(voc.PROPS))

def startup():
    print('Starlette started')

routes = [
    Route('/post', post_method, methods=['POST']),
]

app = Starlette(debug=True, routes=routes, on_startup=[startup])