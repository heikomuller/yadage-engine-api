import os
import shutil
import yaml
import sys
import time

from pymongo import MongoClient
from yadageengine.engine import YADAGEEngine
from yadageengine.workflow import WORKFLOW_ERROR, WORKFLOW_SUCCESS, WORKFLOW_RUNNING


"""Assumes that the template server is running at the follwoing Url."""
CONFIG_FILE = './data/config.yaml'

"""Create a Yadage engine with an empty repository.
"""
# Read configuration
with open(CONFIG_FILE, 'r') as f:
    obj = yaml.load(f.read())
config = {kvp['key'] : kvp['value'] for kvp in obj['properties']}
# Drop database
MongoClient().drop_database(config['mongo.db'])
# Drop workflow directory
workflow_dir = config['db.workdir']
if os.path.isdir(workflow_dir):
    shutil.rmtree(workflow_dir)
os.mkdir(workflow_dir)

engine = YADAGEEngine(config)

wf = engine.create_workflow(sys.argv[1], {})
while not wf['status'] in [WORKFLOW_ERROR, WORKFLOW_SUCCESS]:
    if len(wf['applicableRules']) > 0:
        print 'Apply rules: ' + str(wf['applicableRules'])
        wf = engine.apply_rules(wf['id'], wf['applicableRules'])
    elif len(wf['submittableNodes']) > 0:
        print 'Submit nodes: ' + str(wf['submittableNodes'])
        wf = engine.submit_nodes(wf['id'], wf['submittableNodes'])
    elif wf['status'] == WORKFLOW_RUNNING:
        time.sleep(5)
        wf = engine.get_workflow(wf['id'])
    else:
        print 'Status: ' + wf['status']

print 'Status: ' + wf['status']

MongoClient().drop_database(config['mongo.db'])
if os.path.isdir(workflow_dir):
    shutil.rmtree(workflow_dir)
