import unittest
import json
import os
import shutil
import urllib
import urllib2
import yaml

from pymongo import MongoClient
from yadageengine.engine import YADAGEEngine
from yadageengine.workflow import WORKFLOW_IDLE


"""Assumes that the template server is running at the follwoing Url."""
TEMPLATE_REPOSITORY = 'http://localhost:25012/workflow-repository/api/v1/templates'
TEMPLATE_EXAMPLE = 'http://localhost:25012/workflow-repository/api/v1/templates/madgraph_rivet'
CONFIG_FILE = './data/config.yaml'

class TestYadageEngine(unittest.TestCase):

    def setUp(self):
        """Create a Yadage engine with an empty repository.
        """
        # Read configuration
        with open(CONFIG_FILE, 'r') as f:
            obj = yaml.load(f.read())
        self.config = {kvp['key'] : kvp['value'] for kvp in obj['properties']}
        # Drop database
        MongoClient().drop_database(self.config['mongo.db'])
        # Drop workflow directory
        self.workflow_dir = self.config['db.workdir']
        if os.path.isdir(self.workflow_dir):
            shutil.rmtree(self.workflow_dir)
        os.mkdir(self.workflow_dir)
        self.engine = YADAGEEngine(self.config)

    def tearDown(self):
        #MongoClient().drop_database(self.config['mongo.db'])
        if os.path.isdir(self.workflow_dir):
            shutil.rmtree(self.workflow_dir)

    def test_apply_rules(self):
        """Test the create workflow from template functionality."""
        # Load list of templates
        wf = self.engine.create_workflow(TEMPLATE_EXAMPLE, {})
        app1 = wf['applicableRules']
        wf = self.engine.apply_rules(wf['id'], wf['applicableRules'])
        app2 = wf['applicableRules']
        self.assertEquals(wf['status'], WORKFLOW_IDLE)
        for rule in app1:
            self.assertFalse(rule in app2)
        for rule in app2:
            self.assertFalse(rule in app1)

    def test_create_workflow(self):
        """Test the create workflow from template functionality."""
        # Load list of templates
        wf = self.engine.create_workflow(TEMPLATE_EXAMPLE, {'nevents': 50}, name='MyName')
        self.assertIsNotNone(wf)
        self.assertEquals(wf['name'], 'MyName')
        self.assertEquals(wf['status'], WORKFLOW_IDLE)
        # Make sure that the workflow directory exists
        self.assertTrue(os.path.isdir(os.path.join(self.workflow_dir, wf['id'])))

    def test_delete_workflow(self):
        """Test the delete workflow from template functionality."""
        # Load list of templates
        wf = self.engine.create_workflow(TEMPLATE_EXAMPLE, name='MyName')
        self.assertTrue(self.engine.delete_workflow(wf['id']))
        self.assertFalse(self.engine.delete_workflow(wf['id']))
        # Make sure the workflow directory is deleted as well
        self.assertFalse(os.path.isdir(os.path.join(self.workflow_dir, wf['id'])))

    def test_get_workflow(self):
        """Test the create workflow from template functionality."""
        # Load list of templates
        wf = self.engine.create_workflow(TEMPLATE_EXAMPLE, {'nevents': 50}, name='MyName')
        wf_db = self.engine.get_workflow(wf['id'])
        self.assertIsNotNone(wf_db)
        self.assertEquals(wf_db['name'], 'MyName')
        self.assertEquals(wf_db['status'], WORKFLOW_IDLE)

    def test_list_workflows(self):
        """Test the create workflow from template functionality."""
        templates = [
            temp['links'][0]['href']
                for temp in json.loads(urllib.urlopen(TEMPLATE_REPOSITORY).read())['workflows']
        ]
        workflows = [self.engine.create_workflow(url, {}) for url in templates]
        wf_names = [self.engine.get_workflow(wf['id'])['name'] for wf in workflows]
        self.assertEquals(len(workflows), len(wf_names))
        for wf in workflows:
            self.assertTrue(wf['name'] in wf_names)

if __name__ == '__main__':
    unittest.main()
