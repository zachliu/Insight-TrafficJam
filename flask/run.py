#!/usr/bin/pythoni
import ssl
from app import app
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

################################################################################
ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_ctx.load_cert_chain("/etc/pki/tls/certs/1_trafficjam.online_bundle.crt",
                        "/etc/pki/tls/private/server.key")

# ssl_ctx.verify_mode = ssl.CERT_REQUIRED
ssl_ctx.verify_mode = ssl.CERT_OPTIONAL
# ssl_ctx.verify_mode = ssl.CERT_NONE

# ssl_ctx.load_verify_locations(cafile="/etc/pki/tls/certs/root.crt")   # StartCom Root
# ssl_ctx.load_verify_locations(capath="/etc/ssl/certs/")   # All
# ssl_ctx.load_verify_locations(cafile="/etc/ssl/certs/bc3f2570.0") # Go_Daddy Root
ssl_ctx.load_verify_locations(cafile="/etc/ssl/certs/33815e15.1")   # StartCom Root
# ssl_ctx.load_default_certs(purpose=ssl.Purpose.CLIENT_AUTH)
################################################################################

http_server = HTTPServer(WSGIContainer(app), ssl_options=ssl_ctx)
http_server.listen(443)
IOLoop.instance().start()
