import logging
import json
import os
import sys

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

import pika

import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.concurrent import chain_future, Future

from pika.adapters.tornado_connection import TornadoConnection

PORT = 8888


def async_func(func):

    @gen.coroutine
    def func_wrapper(self, *args, **kwargs):
        future = Future()
        chain_future(self.main_future, future)

        func(self, future, *args, **kwargs)

        result = yield future
        raise gen.Return(result)

    return func_wrapper


class PikaClient(object):

    def __init__(self, ):
        # Default values
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.main_future = Future()

        # A place for us to keep messages sent to us by Rabbitmq
        self.messages = list()

    def _make_response(code, message):
        # TODO:
        pass

    def _get_callback_with_future(self, callback, future):

        def _callback(*args, **kwargs):
            res = callback(*args, **kwargs)
            future.set_result(res)

        return _callback

    # -----------------------------------------------------
    # Connection functions
    # -----------------------------------------------------

    # TODO: make this async
    def connect(self):
        if self.connecting:
            logging.info('Already connecting to RabbitMQ')
            return
        logging.info('Connecting to RabbitMQ on localhost:5672')
        self.connecting = True

        credentials = pika.PlainCredentials('guest', 'guest')
        param = pika.ConnectionParameters(host='localhost',
                                          port=5672,
                                          virtual_host="/",
                                          credentials=credentials)
        self.connection = TornadoConnection(param,
                                            on_open_callback=self.on_connected)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        logging.info('Connected to RabbitMQ on localhost:5672')
        self.connected = True
        self.connection = connection

        self._init_channel()

    def on_closed(self, connection):
        # We've closed our pika connection so stop the demo
        tornado.ioloop.IOLoop.instance().stop()

    # -----------------------------------------------------
    # Channel functions
    # -----------------------------------------------------

    @property
    def channel(self):
        if self._channel is None or not self._channel.is_open:
            self._init_channel()

        return self._channel

    def _init_channel(self):
        self.connection.channel(self.on_channel_open)


    def on_channel_close(self, channel, reply_code, reply_text):
        logging.info('Channel closed, reply_code=%s, reply_text=%s', reply_code,
                     reply_text)
        self.main_future.set_result(reply_code)

    def on_channel_open(self, channel):
        logging.info('Channel Open')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_close)

    # -----------------------------------------------------
    # Queue functions
    # -----------------------------------------------------

    @async_func
    def queue_declare(self, future, queue):
        logging.info('Declaring Queue')

        cb = self._get_callback_with_future(self._on_queue_declare_ok, future)
        self.channel.queue_declare(queue=queue,
                                   durable=True,
                                   callback=cb)

    def _on_queue_declare_ok(self, frame):
        logging.info('Queue Declared')
        return frame

    @async_func
    def queue_bind(self, future, exchange, queue, routing_key):
        logging.info('Binding Queue %s to Exchange %s', queue, exchange)

        cb = self._get_callback_with_future(self.on_queue_bind_ok, future)
        self.channel.queue_bind(exchange=exchange, queue=queue,
                                routing_key=routing_key,
                                callback=cb)

    def on_queue_bind_ok(self, frame):
        logging.info('Queue Bound')
        return frame

    def get_messages():
        self.channel.basic_consume(consumer_callback=self.on_pika_message,
                                   queue=self.queue_name,
                                   no_ack=True)

    def on_pika_message(self, channel, method, header, body):
        logging.info('Message receive, delivery tag #%i' % \
                     method.delivery_tag)
        # Append it to our messages list
        self.messages.append(body)

    def on_basic_cancel(self, frame):
        logging.info('Basic Cancel Ok')
        # If we don't have any more consumer processes running close
        self.connection.close()

    def publish_message(self, exchange, queue):
        # Build a message to publish to RabbitMQ
        body = '%.8f: Request from %s [%s]' % \
               (tornado_request._start_time,
                tornado_request.remote_ip,
                tornado_request.headers.get("User-Agent"))

        # Send the message
        properties = pika.BasicProperties(content_type="text/plain",
                                          delivery_mode=1)
        self.channel.basic_publish(exchange='tornado',
                                   routing_key='tornado.*',
                                   body=body,
                                   properties=properties)


class QueueHandler(tornado.web.RequestHandler):

    @gen.coroutine
    def post(self):
        exchange = 'test123'
        queue_name = 'tornado_test_queue'
        x = yield self.application.pika.queue_declare(queue_name)
        y = yield self.application.pika.queue_bind(exchange, queue_name, queue_name)
        self.write(str(x) + "\n" + str(y))

    def _post_callback(self):
        pass

    def delete(self):
        pass

    def _delete_callback(self):
        pass

    def get(self):
        # Send our output
        self.set_header("Content-type", "application/json")
        self.write(json.dumps(self.application.pika.get_messages()))
        self.finish()


if __name__ == '__main__':

    # Setup the Tornado Application
    settings = {"debug": True}
    application = tornado.web.Application([
        (r"/queue", QueueHandler)
    ], **settings)

    # Helper class PikaClient makes coding async Pika apps in tornado easy
    pc = PikaClient()
    application.pika = pc  # We want a shortcut for below for easier typing

    # Set our logging options
    logging.basicConfig(level=logging.INFO)

    # Start the HTTP Server
    logging.info("Starting Tornado HTTPServer on port %i" % PORT)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(PORT)

    # Get a handle to the instance of IOLoop
    ioloop = tornado.ioloop.IOLoop.instance()

    # Add our Pika connect to the IOLoop since we loop on ioloop.start
    ioloop.add_timeout(1000, application.pika.connect)

    # Start the IOLoop
    ioloop.start()
