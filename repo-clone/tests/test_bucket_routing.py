import unittest

from single_pipeline.bucket_orchestrator import _route_bucket, ROUTING_TABLE, DEFAULT_BUCKET


class TestBucketRouting(unittest.TestCase):
    def test_explicit_mappings(self):
        cases = [
            ("hi", "youth", "HI-YOUTH"),
            ("hi", "news", "HI-NEWS"),
            ("en", "news", "EN-NEWS"),
            ("en", "kids", "EN-KIDS"),
            ("ta", "news", "TA-NEWS"),
            ("bn", "news", "BN-NEWS"),
        ]
        for lang, tone, expected in cases:
            self.assertEqual(_route_bucket(lang, tone), expected)

    def test_default_bucket(self):
        # Unknown tone and language should route to default
        self.assertEqual(_route_bucket("fr", "sports"), DEFAULT_BUCKET)
        self.assertEqual(_route_bucket("", ""), DEFAULT_BUCKET)


if __name__ == "__main__":
    unittest.main()