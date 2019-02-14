import os
import time
import shlex
from datetime import datetime

from django.core.management.base import BaseCommand
from django.conf import settings
from watchdog.observers import Observer
from watchdog.tricks import AutoRestartTrick


class Command(BaseCommand):
    help = 'Start celery workers. Autoreload when file changed.'

    start_celery_cmd = 'celery -A LazyPipeline worker -l INFO'

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.WARNING(
            '[%s] %s' % (self._now, 'starting celery workers by watchdog...')))

        os.chdir(settings.BASE_DIR)

        handler = AutoRestartTrick(
            command=shlex.split(self.start_celery_cmd),
            patterns=['*.py'])
        handler.start()     # start celery

        paths = [
            settings.BASE_DIR + '/LazyPipeline',
            settings.BASE_DIR + '/engine',
        ]

        ob = Observer()
        for path in paths:
            ob.schedule(handler, path, recursive=True)
        ob.start()

        try:
            while True:
                time.sleep(3)
        except KeyboardInterrupt:
            self.stdout.write(self.style.SUCCESS(
                '[%s] %s' % (self._now, 'stopping celery workers...')))
            ob.stop()

        ob.join()
        handler.stop()

    @property
    def _now(self):
        return datetime.now().strftime('%B %d, %Y - %X')
