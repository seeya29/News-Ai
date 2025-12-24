from single_pipeline.fetchers.api_fetchers import DomainAPIFetcher
from single_pipeline.fetchers.rss_fetchers import RSSFetcher


def test_api_fetcher_invalid_url():
    fetcher = DomainAPIFetcher()
    res = fetcher.fetch("http://127.0.0.1:9/")
    assert isinstance(res, dict)
    assert res.get("result") == "error"


def test_rss_fetcher_invalid_url():
    fetcher = RSSFetcher()
    res = fetcher.fetch("http://127.0.0.1:9/")
    assert isinstance(res, dict)
    assert res.get("result") == "error"