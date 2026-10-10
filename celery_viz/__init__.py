"""celery_viz: explain what a Celery workflow is doing, in terms of Celery's own concepts.

Pipeline: raw Celery events -> redaction -> reducer (RunStore) -> derived states -> JSON views.
The state vocabulary lives in states.json (data, not code).
"""
