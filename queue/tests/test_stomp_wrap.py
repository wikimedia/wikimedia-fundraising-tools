import unittest
import time

from queue.stomp_wrap import Stomp

class TestStomp(unittest.TestCase):
    def test_source_meta(self):
        meta = Stomp.source_meta()
        self.assertIsNotNone(meta['source_name'])
        self.assertEqual('audit', meta['source_type'])
        self.assertTrue(int(meta['source_run_id']) > 0)
        self.assertIsNotNone(meta['source_version'])
        self.assertTrue(meta['source_enqueued_time'] >= (time.time() - 60))
        self.assertIsNotNone(meta['source_host'])

if __name__ == '__main__':
    unittest.main()
