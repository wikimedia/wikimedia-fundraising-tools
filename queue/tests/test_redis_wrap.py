from mock import patch

test_queue = "test_queue"

@patch("redis.Redis")
def test_send(MockPyRedis):
    data = dict(a=1)

    with patch("process.globals") as MockGlobals:
        MockGlobals.get_config.return_value.no_effect = False
        MockGlobals.get_config.return_value.redis.queues = {}

        from queue.redis_wrap import Redis
        Redis().send(test_queue, data)

    expected_encoded = '{"a": 1}'
    MockPyRedis().rpush.assert_called_once_with(test_queue, expected_encoded)
