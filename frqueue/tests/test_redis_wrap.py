import json
from unittest import mock
from frqueue.redis_wrap import Redis

# TODO:
# Test source_meta


@mock.patch("redis.Redis")
@mock.patch("process.globals")
def test_send(MockGlobals, MockPyRedis):
    '''
    Data is sent to the queue undisturbed.
    '''
    MockGlobals.get_config.return_value.no_effect = False
    MockGlobals.get_config.return_value.redis.queues = {}

    test_queue = "test_queue"
    data = dict(a=2)

    Redis().send(test_queue, data)

    calls = MockPyRedis().rpush.mock_calls
    assert len(calls) == 1
    # FIXME: indexen arcana.
    actual_message_encoded = calls[0][1][1]
    actual_msg = json.loads(actual_message_encoded)
    assert actual_msg['a'] == 2


@mock.patch("time.time")
@mock.patch("sys.argv")
@mock.patch("os.getpid")
@mock.patch("socket.gethostname")
@mock.patch("redis.Redis")
@mock.patch("process.globals")
def test_source_meta(MockGlobals, MockPyRedis, MockHostname, MockPid, MockArgv, MockTime):
    '''
    Source meta fields are built correctly
    '''
    MockGlobals.get_config.return_value.no_effect = False
    MockGlobals.get_config.return_value.redis.queues = {}
    MockHostname.return_value = "localhost-test"
    MockPid.return_value = 123
    # Can't use __file__ because of .py vs .pyc on Python 2.
    MockArgv.__getitem__.return_value = "test_redis_wrap"
    MockTime.return_value = 1476388000

    test_queue = "test_queue"
    data = dict(a=1)

    expected = {"a": 1, "source_name": "test_redis_wrap", "source_version": "unknown", "source_enqueued_time": 1476388000, "source_host": "localhost-test", "source_run_id": 123, "source_type": "audit"}

    Redis().send(test_queue, data)

    calls = MockPyRedis().rpush.mock_calls
    assert len(calls) == 1
    # FIXME: indexen arcana.
    actual_message_encoded = calls[0][1][1]
    actual_msg = json.loads(actual_message_encoded)
    assert actual_msg == expected
