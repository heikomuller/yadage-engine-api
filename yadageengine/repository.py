"""YADAGE Workflow Repository

Manage and persist information about running and completed workflow instances.
Defines the Workflow Repository API.
"""

from abc import abstractmethod

import workflow

# ------------------------------------------------------------------------------
#
# Instance Manager
#
# ------------------------------------------------------------------------------
class WorkflowRepository(object):
    """Abstract repository class. Defines the interface methods that every
    workflow repository manager has to implement.
    """
    @abstractmethod
    def create_workflow(self, workflow_id, name, state, workflow_json):
        """Create a new workflow instance for a given Json representation of the
        workflow object. Assigns the given name with the new workflow instance.
        The workflow repository manager is agnostic to the structure and content
        of the workflow Json object at this point.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : workflow.WorkflowState
            Last workflow state (not necessarily reflective of all task states).
        workflow_json : Json object
            Json serialization of yadage workflow instance

        Returns
        -------
        workflow.WorkflowDBInstance
            Instance object for created workflow
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def list_workflows(self, state=None):
        """Get a list of all workflow instances in the repository. Allows to
        filter result by state.

        Parameters
        ----------
        state : workflow.WorkflowState, optional
            Workflow state to filter by

        Returns
        -------
        List [workflow.WorkflowDBInstance]
            Workflow instances for all workflows in repository (that match
            filter, if given)
        """
        pass

    @abstractmethod
    def update_workflow(self, workflow_id, name, state, workflow_json):
        """Replace existing workflow instance with given workflow instance.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : workflow.WorkflowState
            Last workflow state (not necessarily reflective of all task states).
        workflow_json : Json object
            Json serialization of yadage workflow instance

        Returns
        -------
        workflow.WorkflowInstance
            The updated workflow instance or None if no existing workflow
            matched the identifier
        """
        pass


class MongoDBWorkflowRepository(WorkflowRepository):
    """Persistent implementation of a workflow repository manager using MongoDB.

    Attributes
    ----------
    collection : Collection
        Collection in MongoDB where workflow instances are stored
    """
    def __init__(self, mongo_collection):
        """Intialize the MongoDB collection that contains workflow instance
        objects.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB where workflow instances are stored
        """
        self.collection = mongo_collection

    def create_workflow(self, workflow_id, name, state, workflow_json):
        """Override WorkflowInstanceManager.create_workflow."""
        # Create a new entry in the workflow collection and return new instance
        self.collection.insert_one(
            self.get_json_from_instance(
                workflow_id, name, state, workflow_json
            )
        )
        return workflow.WorkflowDBInstance(
            workflow_id,
            name,
            state,
            workflow_json
        )

    def delete_workflow(self, workflow_id):
        """Override WorkflowInstanceManager.delete_workflow."""
        # Delete object with given identifier. Result contains object count
        # to determine if the object existed or not
        result = self.collection.delete_one({'_id': workflow_id})
        return result.deleted_count > 0

    @staticmethod
    def get_instance_from_json(obj):
        """Static method that creates a WorkflowInstance object from a Json
        object as stored in the database.

        Parameters
        ----------
        obj : Json 0bject
            Json serialization of workflow instances in repository

        Returns
        -------
        workflow.WorkflowDBInstance
            Workflow instance object
        """
        identifier = obj['_id']
        name = obj['name']
        state = workflow.WORKFLOW_STATES[obj['state']]
        workflow_json = obj['workflow']
        return workflow.WorkflowDBInstance(
            identifier,
            name,
            state,
            workflow_json
        )

    def get_workflow(self, workflow_id):
        """Override WorkflowInstanceManager.get_workflow."""
        # Find all objects with given identifier. The result size is expected
        # to be zero or one. We only look at the first object of the result (if
        # any).
        cursor = self.collection.find({'_id': workflow_id})
        if cursor.count() > 0:
            return self.get_instance_from_json(cursor.next())
        else:
            return None

    def list_workflows(self, state=None):
        """Override WorkflowInstanceManager.list_workflows."""
        result = []
        # Build the document query. Select all entries if no state filter is
        # given.
        if not state is None:
            cursor = self.collection.find( {'state' : state.name})
        else:
            cursor = self.collection.find()
        # Iterate over all objects in the query result. Add a workflow instance
        # object for each to the returned result
        for document in cursor:
            result.append(self.get_instance_from_json(document))
        return result

    @staticmethod
    def get_json_from_instance(identifier, name, state, workflow_json):
        """Create a Json object from all components of a workflow instance.
        Use to generate the Json objects that are stored in MongoDB.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : workflow.WorkflowState
            Last workflow state (not necessarily reflective of all task states).
        workflow_json : Json object
            Json serialization of yadage workflow instance

        Returns
        -------
        Json object
            Json document to be stored in MongoDB
        """
        return {
            '_id' : identifier,
            'name' : name,
            'state' : state.name,
            'workflow' : workflow_json
        }

    def update_workflow(self, workflow_id, name, state, workflow_json):
        """Override WorkflowInstanceManager.update_workflow."""
        obj = self.get_json_from_instance(
            workflow_id,
            name,
            state,
            workflow_json
        )
        # Replace document with given identfier. the result contains a counter
        # that is used to derive whether the instance identifier referenced an
        # existing workflow instance.
        result = self.collection.replace_one({'_id' : workflow_id}, obj)
        if result.matched_count > 0:
            return workflow.WorkflowDBInstance(
                workflow_id,
                name,
                state,
                workflow_json
            )
        else:
            return None
