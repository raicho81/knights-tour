from celery import Celery
from dynaconf import settings

app = Celery('knights_tour_tasks',
             broker=settings.CELERY_TASKS_BROKER,
             backend=settings.CELERY_TASKS_BACKEND,
             include=['celery_tasks.tasks'],
             accept_content=['msgpack', "json"],
             task_compression="gzip",
             result_compression="gzip")

app.conf.task_serializer = 'msgpack'
app.conf.result_serializer = 'msgpack'

if __name__ == '__main__':
    app.start()