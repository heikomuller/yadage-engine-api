"""Workflow objects managed by the Yadage Engine and stored in the workflow
respository.

Defines workflow descriptors and workflow instance objects. Descriptors contain
basic information about workflow instances, i.e., identifier, name, and program
execution status. Workflow instance objects contain the full information about
the workflow including the internal execution state.

The workflow repository manages and persists information about running and
completed workflow instances. The default repository implementation uses
MongoDB as the storage backend.
"""
import datetime
import functools
import os
import uuid

from bson.objectid import ObjectId
from pymongo import MongoClient

from packtivity.asyncbackends import CeleryBackend
import packtivity.statecontexts.posixfs_context as statecontext
import yadage.backends.packtivitybackend as pb
import yadage.clihelpers as clihelpers
from yadage.controllers import PersistentController, VariableProxy, setup_controller_fromstring
from yadage.yadagemodels import YadageWorkflow

# ------------------------------------------------------------------------------
#
# Constants
#
# -----------------------------------------------------------------------------

"""Define possible workflow statuses. A workflow can be in either of four
different states: RUNNING (i.e., submitted task that is running), WAITING
(i.e., waiting for user interaction), FAILED (i.e., execution of at least on
task falied), and SUCCESS (i.e., workflow successfully completed).
"""
WORKFLOW_RUNNING ='RUNNING'
WORKFLOW_IDLE ='IDLE'
WORKFLOW_ERROR  = 'ERROR'
WORKFLOW_SUCCESS = 'SUCCESS'

WORKFLOW_STATES = [WORKFLOW_RUNNING, WORKFLOW_IDLE, WORKFLOW_ERROR, WORKFLOW_SUCCESS]

# ------------------------------------------------------------------------------
#
# Workflow Instances
#
# ------------------------------------------------------------------------------

class WorkflowInstance(object):
    """Full workflow instance object. Extends the workflow descriptor with the
    internal state of workflow execution.

    Attributes
    ----------
    identifier : string
        Unique workflow identifier
    name : string
        User-defined workflow name
    status : string
        Workflow state object
    createdAt : string
        Timestamp of creation (UTC)
    json : Json object
        Json serialization of workflow state object
    applicable_rules : list(adage.Rule)
        List of applicable rules
    submittable_nodes : list(adage.AdageNode)
        List of submittable nodes
    """
    def __init__(self, metadata, db, backend):
        """Initialize the identfifier, name, state, dag, rules, applied rules
        and applicable rule identifier. At this stage all ADAGE objects are
        simply Json objects.

        Parameters
        ----------
        metadata : dict
            Metadata information including workflow id and name
        program_state : YadageWorkflow
            Internal state of workflow execution
        db : WorkflowRepository
            Workflow repository to get and update workflow state
        backend : packtivity.PythonCallableAsyncBackend
            Default Yadage backend
        """
        self.identifier = str(metadata['_id'])
        self.name = metadata['name']
        self.createdAt = metadata['createdAt']
        self.db = db
        self.wflowid = metadata['workflow']
        self.deserializer = functools.partial(
            load_state_custom_deserializer,
            backend=backend
        )
        try:
            self.controller = PersistentController(self)
            self.controller.backend = backend
            #    pass
            # Get the list of identifier for rules that are applicable.
            self.applicable_rules = self.controller.applicable_rules()
            # Get list of identifier for submittable nodes
            self.submittable_nodes = self.controller.submittable_nodes()
            # Set the workflow status
            if self.controller.validate():
                if self.controller.finished():
                    if self.controller.successful():
                        self.status = WORKFLOW_SUCCESS
                    else:
                        self.status = WORKFLOW_ERROR
                else:
                    if len(self.applicable_rules) > 0 or len(self.submittable_nodes) > 0:
                        self.status = WORKFLOW_IDLE
                    else:
                        self.status = WORKFLOW_RUNNING
            else:
                self.status = WORKFLOW_ERROR
        except AttributeError as ex:
            # Set status to error if the workflow cannot be initialized
            self.applicable_rules = []
            self.submittable_nodes = []
            self.status = WORKFLOW_ERROR

    def apply_rules(self, rule_instances):
        """Apply a given set of rule instances.

        Raises ValueError if any of the selected rules is not applicable.

        Parameters
        ----------
        rule_instances : list(string)
            List of rule identifier
        """
        # Ensure that all selected rules are applicable and that there are
        # no duplicates in the list
        rules = set()
        for rule_id in rule_instances:
            if not rule_id in self.applicable_rules:
                raise ValueError('not applicable: ' + rule_id)
            elif rule_id in rules:
                raise ValueError('duplicate rule: ' + rule_id)
            rules.add(rule_id)
        # Apply the list of rules
        self.controller.apply_rules(rule_instances)

    def commit(self, data):
        """Update workflow state. Implements method from
        yadage.controllers.MongoBackedModel.

        Parameters
        ----------
        data : yadage.YadageWorkflow
            Yadage workflow object
        """
        self.db.update_Workflow(self.wflowid, data.json())

    def json(self):
        """Retrieve workflow state. Implements method from
        yadage.controllers.MongoBackedModel.
        """
        return self.db.get_workflow_state(self.wflowid)

    def load(self):
        """Retrieve workflow state. Implements method from
        yadage.controllers.MongoBackedModel.
        """
        return self.deserializer(self.json())

    def submit_nodes(self, node_instances):
        """Submit a given set of node instances.

        Raises ValueError if any of the selected nodes is not submittable.

        Parameters
        ----------
        node_instances : list(string)
            List of node identifier
        """
        # Ensure that all selected nodes are submitttable and that there are
        # no duplicates in the list
        nodes = set()
        for node_id in node_instances:
            if not node_id in self.submittable_nodes:
                raise ValueError('not submittable: ' + node_id)
            elif node_id in nodes:
                raise ValueError('duplicate node: ' + node_id)
            nodes.add(node_id)
        # Submit the list of nodes
        self.controller.submit_nodes(node_instances)


# ------------------------------------------------------------------------------
#
# Workflow Repository
#
# ------------------------------------------------------------------------------

class WorkflowRepository(object):
    """Persistent implementation of a workflow repository manager using MongoDB.

    Attributes
    ----------
    connector : MongoDBFactory
        Connector for MongoDB database
    workflow_dir : string
        Base directory for all workflow files
    """
    def __init__(self, config):
        """Initialize the database connector and workflow directory.

        Parameters
        ----------
        config : dict
            Yadage configuration parameters
        """
        # Initialize the MongoDB connector
        self.store = MongoDBConnector(config)
        # Directory for workflow files
        self.workflow_dir = os.path.abspath(config['db.workdir'])
        # Set the default Yadage backend. This implementation uses Celery
        self.backend = pb.PacktivityBackend(packtivity_backend=CeleryBackend())

    def create_workflow(self, workflow_template, name, init_data):
        """Create a new workflow instance in the repository. Assigns the given
        identifier and name to the new workflow instance.

        Parameters
        ----------
        workflow_template : dict
            Serialization of the workflow template
        name : string
            User-defined workflow name
        init_data : dict
            Dictionary of user-provided workflow arguments

        Returns
        -------
        workflow.WorkflowInstance
            Descriptor for workflow instance
        """
        # Generate a unique identifier for the new workflow instance
        identifier = str(uuid.uuid4())
        # Create a new directory in the workflow base directory with the
        # workflow identifier as the directory name.
        workdir = os.path.join(self.workflow_dir, identifier)
        os.makedirs(workdir)
        rootcontext = statecontext.merge_contexts(
            {},
            statecontext.make_new_context(workdir)
        )
        # Create Yadage workflow object from template and initialize with user
        # provided arguments
        workflowobj = YadageWorkflow.createFromJSON(
            workflow_template,
            rootcontext
        )
        workflowobj.view().init(init_data)
        # Timestamp of object creation
        timestamp = datetime.datetime.utcnow().isoformat()
        # Connect to MongoDB. Insert workflow state inti collection workflows
        # and metadata inti collection metadata
        db = self.store.get_database()
        metadata = {
            '_id' : identifier,
            'name' : name,
            'status' : WORKFLOW_IDLE,
            'createdAt' : timestamp,
            'workflow' : str(
                db.workflows.insert_one(workflowobj.json()).inserted_id
            )
        }
        db.metadata.insert_one(metadata)
        return WorkflowInstance(metadata, self, self.backend)

    def delete_workflow(self, workflow_id):
        """Delete workflow instance with the given identifier. The result
        indicates whether the given workflow identifier was valid (i.e.,
        identified an existing workflow) or not.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        Boolean
            True, if worlflow deleted, False if not found
        """
        # Connect to database
        db = self.store.get_database()
        # Retrieve metadata information for given workflow. Return False if it
        # does not exist
        cursor = db.metadata.find({'_id': workflow_id})
        if cursor.count() == 0:
            return False
        md = cursor.next()
        # Delete workflow and metadata
        db.workflows.delete_one({'_id': md['workflow']})
        db.metadata.delete_one({'_id': workflow_id})
        return True

    def get_workflow(self, workflow_id):
        """Get workflow instance with given identifier.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        workflow.WorkflowInstance
            Workflow instance or None
        """
        db = self.store.get_database()
        cursor = db.metadata.find({'_id': workflow_id})
        if cursor.count() > 0:
            obj = cursor.next()
            return WorkflowInstance(obj, self, self.backend)
        else:
            return None

    def get_workflow_state(self, workflow_id):
        """Get the state for the workflow with given identifier.

        Parameters
        ----------
        workflow_id : string
            Unique workflow state identifier.  Note, this is not the workflow
            identifier that is visible to the user but the identifier of the
            MongoDB document instead.

        Returns
        -------
        Json document
        """
        # Connect to database
        db = self.store.get_database()
        return db.workflows.find_one({'_id' : ObjectId(workflow_id)})

    def get_workflow_stats(self):
        """Get a count of workflows in the database by workflow status.

        Returns
        -------
        dict
            Dictionary containing dictionary of workflow status counts
        """
        statistics = {status : 0 for status in WORKFLOW_STATES}
        for workflow in self.list_workflows():
            statistics[workflow.status] += 1
            self.stats = statistics
        return statistics

    def list_workflows(self, status=None):
        """List all workflow instances in the repository. Allows to filter the
        result by workflow status.

        Parameters
        ----------
        status : string, optional
            Workflow status to filter by

        Returns
        -------
        list(WorkflowInstance)
            Descriptors for workflow instances in the repository
        """
        result = []
        # Iterate over all metadata objects and generate the workflow instances.
        # The status filter can only be applied after that
        db = self.store.get_database()
        cursor = db.metadata.find()
        for document in cursor:
            wf = WorkflowInstance(document, self, self.backend)
            if not status is None:
                if status != wf.status:
                    continue
            result.append(wf)
        return result

    def update_Workflow(self, workflow_id, data):
        """Update the state of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id : Unique workflow state identifier. Note, this is not the
            identifier of the workflow object that is visible to the user but
            the MongoDB object id
        data : JSON
            JSON representation of the workflow state
        """
        # Connect to database
        db = self.store.get_database()
        # Update workflow state in workflow collection
        db.workflows.replace_one({'_id' : ObjectId(workflow_id)}, data)
        # Update the last modified date in the metadata
        #timestamp = datetime.datetime.utcnow().isoformat()
        #db.metadata.update_one(
        #    { 'workflow' : workflow_id },
        #    {'$set': { 'lastModifiedAt' : timestamp }}
        #)


# ------------------------------------------------------------------------------
# MongoDB Connector
# ------------------------------------------------------------------------------

class MongoDBConnector(object):
    """Factory pattern to establish connection to the Mongo database used
    by the Yadage Web API.
    """
    def __init__(self, config):
        """Initialize the database name and connection Uri. Uses the follwing
        parameters:

        * mongo.db (optional)  : Name of MongoDB database to store workflow
                                 information. Default is 'yadage'.
        * mongo.uri (optional) : Connection string. Default is to connect to
                                 local instance on default port.

        Parameters
        ----------
        config : dict
            Yadage configuration parameters
        """
        self.db_uri = config['mongo.uri'] if 'mongo.uri' in config else None
        self.db_name = config['mongo.db'] if 'mongo.db' in config else 'yadage'

    def get_database(self):
        """Connect to MongoDB and return database object.

        Returns
        -------
        MongoDb.database
            MongoDB database object
        """
        if not self.db_uri is None:
            return MongoClient(self.db_uri)[self.db_name]
        else:
            return MongoClient()[self.db_name]


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def load_state_custom_deserializer(jsondata, backend=None):
    return YadageWorkflow.fromJSON(
        jsondata,
        VariableProxy,
        backend
    )
