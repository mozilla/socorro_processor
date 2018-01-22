# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import random
import sys
import uuid

import pytest


# Insert parent directory in sys.path so we can import pigeon
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


# Insert test env variables
os.environ['PIGEON_USER'] = os.environ['RABBITMQ_DEFAULT_USER']
os.environ['PIGEON_PASSWORD'] = os.environ['RABBITMQ_DEFAULT_PASS']
os.environ['PIGEON_VIRTUAL_HOST'] = os.environ['RABBITMQ_DEFAULT_VHOST']
os.environ['PIGEON_HOST'] = os.environ['RABBITMQ_HOST']
os.environ['PIGEON_PORT'] = '5672'
os.environ['PIGEON_QUEUE'] = 'socorrodev.normal'


from pigeon import build_pika_connection, CONFIG, handler  # noqa


class LambdaContext:
    """Context class that mimics the AWS Lambda context

    http://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    """
    def __init__(self):
        self.aws_request_id = uuid.uuid4().hex

        self.log_group_name = '/aws/lambda/test'
        self.log_stream_name = '2016-11-15blahblah'

        self.function_name = 'test'
        self.memory_limit_in_mb = '384'
        self.function_version = '1'
        self.invoked_function_arn = 'arn:aws:lambda:us-west-2:blahblah:function:test'

        # FIXME(willkg): Keeping these as None until we need them.
        self.client_context = None
        self.identity = None

    def get_remaining_time_in_millis(self):
        # FIXME(willkg): Implement this when we need it
        return 5000


class PigeonClient:
    """Class for pigeon in the AWS lambda environment"""
    def crash_id_to_path(self, crash_id):
        return 'v2/raw_crash/{entropy}/{date}/{crashid}'.format(
            entropy=crash_id[0:3],
            date='20' + crash_id[-6:],
            crashid=crash_id
        )

    def build_crash_save_events(self, keys):
        if isinstance(keys, str):
            keys = [keys]

        # FIXME(willkg): This only generates a record that has the stuff that
        # pigeon is looking for. It's not a full record.
        return {
            'Records': [
                {
                    'eventSource': 'aws:s3',
                    'eventName': 'ObjectCreated:Put',
                    's3': {
                        'object': {
                            'key': key
                        }
                    }
                }
                for key in keys
            ]
        }

    def run(self, events):
        result = handler(events, LambdaContext())
        return result


@pytest.fixture
def client():
    """Returns an AWS Lambda mock that runs pigeon thing"""
    return PigeonClient()


class RabbitMQHelper:
    def __init__(self):
        self.build_conn()

    def build_conn(self):
        """This builds a connection based on CONFIG values"""
        self.conn = build_pika_connection(
            CONFIG.host,
            CONFIG.port,
            CONFIG.virtual_host,
            CONFIG.user,
            CONFIG.password
        )
        self.clear_queues()
        self.declare_queues()

    def declare_queues(self):
        channel = self.conn.channel()
        for throttle, queue in CONFIG.queues:
            channel.queue_declare(queue=queue, durable=True)
        channel.close()

    def clear_queues(self):
        channel = self.conn.channel()
        for throttle, queue in CONFIG.queues:
            channel.queue_delete(queue=queue)
        channel.close()

    def next_item(self, queue=None):
        if queue is None:
            # If queue is None, then take the first (and possibly only) queue
            # from self.queues
            queue = CONFIG.queues[0][1]
        channel = self.conn.channel()
        method_frame, header_frame, body = channel.basic_get(queue=queue)
        if method_frame:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return body.decode('ascii')
        return None


@pytest.fixture
def rabbitmq_helper():
    """Returns a RabbitMQ helper instance based on current CONFIG values"""
    return RabbitMQHelper()


@pytest.yield_fixture
def mock_randint_always_20():
    """Mocks random.randint to always return 20"""
    old_randint = random.randint

    def mock_randint(a, b):
        print('MOCKED RANDINT')
        return 20

    random.randint = mock_randint
    yield
    random.randint = old_randint
