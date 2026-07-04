from pymonque import BaseQueue, Task, task, utc_now

from mongomock import MongoClient
from datetime import datetime
import time

client = MongoClient()
db = client["test"]

class Queue(BaseQueue):
    @task
    @staticmethod
    def hello(msg: str):
        print(f"Hello, {msg}!")

q = Queue(db)

def test_indexes():
    q.task.createIndexes()
    q.scheduler.createIndexes()


def test_scheduling():
    work = Queue.hello
    q.task.schedule(work(msg="a"), deadline=utc_now())
    assert len(list(q.task.tasksCollection.find())) >= 1


def test_workers():
    q.startWorkers(2, 0)
    time.sleep(0.1)

    for t in q.task.tasksCollection.find():
        tsk = Task.model_validate(t)
        assert tsk.status == "success"
