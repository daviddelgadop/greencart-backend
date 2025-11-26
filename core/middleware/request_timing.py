# core/middleware/request_timing.py
import time
import re
import logging
from collections import Counter, defaultdict
from django.db import connection
from django.utils.deprecation import MiddlewareMixin
from django.conf import settings

logger = logging.getLogger(__name__)

SQL_LITERAL_RE = re.compile(r"(\b\d+\b)|('[^']*')|(\"[^\"]*\")", re.MULTILINE)

def _normalize_sql(sql: str) -> str:
    """
    Roughly normalize SQL to detect duplicates:
    - strip extra whitespace
    - replace literals and quoted strings with a placeholder
    """
    compact = " ".join(sql.split())
    return SQL_LITERAL_RE.sub("?", compact)

class DetailedRequestTimingMiddleware(MiddlewareMixin):
    """
    Logs:
      - total request duration
      - DB query count and total DB time (sum of per-query durations)
      - top N slow queries
      - top N duplicate queries (normalized fingerprint)
    Only prints details when the request exceeds SLOW_MS (default 300 ms),
    but always logs a one-line summary.
    Requires DEBUG=True to populate connection.queries (or DB debug logging).
    """

    SLOW_MS = getattr(settings, "REQUEST_TIMING_SLOW_MS", 300)
    TOP_N = getattr(settings, "REQUEST_TIMING_TOP_N", 5)

    def process_request(self, request):
        request._rt_start = time.perf_counter()
        # Snapshot current query list length so we only consider queries from this request
        request._rt_q_start = len(connection.queries)

    def process_response(self, request, response):
        try:
            total_ms = (time.perf_counter() - getattr(request, "_rt_start", time.perf_counter())) * 1000.0
            q_start = getattr(request, "_rt_q_start", 0)
            queries = connection.queries[q_start:] if hasattr(connection, "queries") else []
            q_count = len(queries)

            # Sum DB time (Django stores it as string seconds)
            db_ms = 0.0
            for q in queries:
                try:
                    db_ms += float(q.get("time", 0.0)) * 1000.0
                except Exception:
                    pass

            # One-line summary (always)
            logger.info("TIMING %s %s — %.1f ms total — %.1f ms db — %d queries — %s %s",
                        request.method,
                        request.path,
                        total_ms,
                        db_ms,
                        q_count,
                        response.status_code,
                        response.get("Content-Type", "")[:40])

            # Detailed breakdown only for slow requests
            if total_ms >= self.SLOW_MS and q_count > 0:
                # Top slow queries (by duration)
                slow = sorted(queries, key=lambda q: float(q.get("time", 0.0)), reverse=True)[: self.TOP_N]
                logger.warning("SLOW REQ %s %s — total %.1f ms — db %.1f ms — %d queries",
                               request.method, request.path, total_ms, db_ms, q_count)

                for i, q in enumerate(slow, 1):
                    try:
                        ms = float(q.get("time", 0.0)) * 1000.0
                    except Exception:
                        ms = -1
                    sql = q.get("sql", "").strip()
                    logger.warning("  #%d slow — %.1f ms — %s", i, ms, sql)

                # Duplicate query fingerprints (detect N+1)
                counts = Counter()
                per_fprint = defaultdict(list)
                for q in queries:
                    fp = _normalize_sql(q.get("sql", ""))
                    counts[fp] += 1
                    per_fprint[fp].append(q)

                dupes = counts.most_common(self.TOP_N)
                if dupes:
                    logger.warning("  Top duplicate queries (normalized):")
                    for (fp, n) in dupes:
                        if n <= 1:
                            continue
                        # sum time for this fingerprint
                        t_ms = sum(float(q.get("time", 0.0)) * 1000.0 for q in per_fprint[fp])
                        logger.warning("    ×%d — %.1f ms total — %s", n, t_ms, fp)

        except Exception:
            # Never break the response due to logging
            pass
        return response
