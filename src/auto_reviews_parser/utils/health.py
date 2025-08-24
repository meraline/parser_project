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


def health_check():
    """Health check для мониторинга"""
    try:
        container = Container()
        db = container.database()
        stats = db.get_parsing_stats()
        return {
            "status": "healthy",
            "database": True,
            "total_reviews": stats.get("total_reviews", 0),
            "metrics_url": f"http://localhost:{os.getenv('PROMETHEUS_PORT', 8000)}/metrics",
        }
    except Exception as e:
        return {"status": "unhealthy", "database": False, "error": str(e)}
