import os
from celery import Celery

CELERY_APP = os.environ.get('CELERY_APP', 'bash')
CELERY_CONCURRENCY = os.environ.get('CELERY_CONCURRENCY', 3)

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
BASH_TIMEOUT = os.environ.get('BASH_TIMEOUT', 1200)

app = Celery(CELERY_APP, broker=CELERY_BROKER_URL, backend=CELERY_BROKER_URL)
app.conf.CELERY_CONCURRENCY = 3
app.conf.CELERY_ACKS_LATE = True
app.conf.CELERYD_PREFETCH_MULTIPLIER = 1

@app.task
def exponent(x, y):
    return x ** y