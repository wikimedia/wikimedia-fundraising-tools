import time

from queue.stomp_wrap import Stomp


def test_source_meta():
    meta = Stomp.source_meta()
    assert meta['source_name'] is not None
    assert 'audit' == meta['source_type']
    assert int(meta['source_run_id']) > 0
    assert meta['source_version'] is not None
    assert meta['source_enqueued_time'] >= (time.time() - 60)
    assert meta['source_host'] is not None
