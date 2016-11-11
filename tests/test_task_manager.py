import os
import sys
sys.path.insert(0, os.path.abspath('..'))

from pymongo import MongoClient
import yadageengine.task as tasks

tasks = tasks.MongoDBTaskManager(MongoClient().yadage.taskstest)
tasks.create_task('WF1', 'T1')
tasks.create_task('WF2', 'T1')
print tasks.has_tasks('WF1')
print tasks.has_tasks('WF2')
print tasks.has_tasks('WF3')
print tasks.list_tasks('WF1')
print tasks.list_workflows()
tasks.delete_task('WF1', 'T1')
print tasks.has_tasks('WF1')
print tasks.list_tasks('WF1')
print tasks.list_workflows()
tasks.delete_task('WF2', 'T1')
print tasks.has_tasks('WF2')
