from __future__ import absolute_import, unicode_literals

from django.core.management import BaseCommand, CommandError

from eb_sqs.worker.service import WorkerService
from eb_sqs.settings import CONSUME_QUEUE_NAMES as CQN

class Command(BaseCommand):
    help = 'Command to process tasks from one or more SQS queues'

    def add_arguments(self, parser):
        parser.add_argument('--queues', '-q',
                            dest='queue_names',
                            help='Name of queues to process, separated by commas')

    def handle(self, *args, **options):
        queue_names_str = options.get('queue_names',CQN)
        
        if queue_names_str == "":
            raise CommandError('Queue names (--queues) not specified')
            
        queue_names = [queue_name.rstrip() for queue_name in queue_names_str.split(',')]

        WorkerService().process_queues(queue_names)
