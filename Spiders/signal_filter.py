from scrapy.dupefilters import RFPDupeFilter
from scrapy_splash.dupefilter import splash_request_fingerprint

class CustomFilter(RFPDupeFilter):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler=None):
        if crawler:
            fingerprinter = crawler.request_fingerprinter
            super().__init__(fingerprinter=fingerprinter)
            self.crawler = crawler
        else:
            super().__init__()

    def request_fingerprint(self, request):
        """Generate a fingerprint for a given request.
        
        This method overrides the deprecated request_fingerprint() with the new
        fingerprint() method, while maintaining compatibility with splash requests.
        """
        if 'splash' in request.meta:
            return splash_request_fingerprint(request)
        return self.crawler.request_fingerprinter.fingerprint(request)