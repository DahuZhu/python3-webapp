import logging; logging.basicConfig(level=logging.INFO)
import asyncio
from aiohttp import web

## define server's return
async def index(request):
    return web.Response(body=b'<h1>Awesome Website</h1>', content_type='text/html')

## build server app, minitor port 9000
def init():
    app = web.Application()
    app.router.add_get('/', index)
    web.run_app(app, host='127.0.0.1', port=9000)

if __name__ == "__main__":
    init()