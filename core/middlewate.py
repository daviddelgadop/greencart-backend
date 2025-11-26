import time
from django.db import connection
from django.utils.deprecation import MiddlewareMixin
import logging

logger = logging.getLogger(__name__)

class RequestTimingMiddleware(MiddlewareMixin):
    def process_request(self, request):
        request._start_time = time.perf_counter()
        request._start_queries = len(connection.queries)

    def process_response(self, request, response):
        try:
            duration = (time.perf_counter() - request._start_time) * 1000
            qcount = len(connection.queries) - getattr(request, "_start_queries", 0)
            logger.info("TIMING %s %s — %.1f ms — %d queries",
                        request.method, request.path, duration, qcount)
        except Exception:
            pass
        return response