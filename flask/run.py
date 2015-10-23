#!/usr/bin/python
from app import app
#app.run(host='0.0.0.0', port=5000, debug=True)
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

http_server = HTTPServer(WSGIContainer(app))
http_server.listen(5000)
IOLoop.instance().start()
