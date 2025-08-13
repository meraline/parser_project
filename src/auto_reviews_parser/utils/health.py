from __future__ import annotations

from ..services.auto_reviews_parser import ReviewsDatabase


METRICS_URL = "http://localhost:8000/metrics"


def health_check(db: ReviewsDatabase) -> dict:
    """Perform a simple health check for the service.

    The function verifies database connectivity and returns a status
    dictionary containing a link to the Prometheus metrics endpoint.
    """
    try:
        # simple query to ensure database is reachable
        db.get_reviews_count()
        db_ok = True
    except Exception:
        db_ok = False

    status = "ok" if db_ok else "error"
    return {"status": status, "database": db_ok, "metrics": METRICS_URL}
