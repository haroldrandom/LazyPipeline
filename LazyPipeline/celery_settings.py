import os
from celery import Celery

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'LazyPipeline.settings')

app = Celery('LazyPipeline')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# TODO wired but seems the only effective way to set result expiration
app.conf.result_expires = 30

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def task_greeting_lazy_people(self):
    return {'message': 'Hi, lazy people!'}
