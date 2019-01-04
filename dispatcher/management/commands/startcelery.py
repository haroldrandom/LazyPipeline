import shlex
import subprocess
from datetime import datetime

from django.core.management.base import BaseCommand
from django.utils import autoreload


class Command(BaseCommand):
    help = 'Start celery workers. Autoreload when file changed.'

    def handle(self, *args, **kwargs):
        autoreload.main(self._restart_celery_worker)

    @property
    def _now(self):
        return datetime.now().strftime('%B %d, %Y - %X')

    def _restart_celery_worker(self):
        txt = '[%s] %s' % (self._now, 'stopping celery workers...')
        self.stdout.write(self.style.WARNING(txt))
        self._run_shell_cmd('pkill celery')

        txt = '[%s] %s' % (self._now, 'starting celery workers...')
        self.stdout.write(self.style.SUCCESS(txt))
        self._run_shell_cmd('celery -A LazyPipeline worker -B -l INFO')

    @staticmethod
    def _run_shell_cmd(cmd):
        subprocess.call(shlex.split(cmd))
