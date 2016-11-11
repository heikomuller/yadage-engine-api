"""Yadage Task manager

Maintains information about scheduled and running tasks. The task manager is
used by the Yadage Engine to keep track of tasks that have been submitted and
are not yet completed.
"""

from abc import abstractmethod
import pymongo


class TaskManager(object):
    """Abstract task manager class. Defines the interface methods that every
    task manager will implement.
    """
    @abstractmethod
    def create_task(self, workflow_id, node_id):
        """Create an entry for a new task that has been submitted.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        node_id : string
            Unique node / task identifier
        """
        pass

    @abstractmethod
    def delete_task(self, workflow_id, node_id):
        """Delete entry from repository.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        node_id : string
            Unique node / task identifier
        """
        pass

    @abstractmethod
    def delete_tasks(self, workflow_id):
        """Delete all tasks associated with the given workflow.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        """
        pass

    @abstractmethod
    def has_tasks(self, workflow_id):
        """Delete entry from repository.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        Boolean
            True if there exist entries for the given workflow
        """

    @abstractmethod
    def list_tasks(self, workflow_id):
        """Get existing task for workflow.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        List(string)
            List of node / task identifier for workflow
        """
        pass

    @abstractmethod
    def list_workflows(self):
        """Get workflows that have tasks defined for them.

        Returns
        -------
        List(string)
            List of workflow identifier
        """
        pass

# ------------------------------------------------------------------------------
#
# CLASS: MongoDBInstanceManager
#
# Persistent implementation of a workflow instance manager using MongoDB
#
# ------------------------------------------------------------------------------
class MongoDBTaskManager(TaskManager):
    """Implementation of task manager interface using MongoDB as persistent
    data store.

    Attributes
    ----------
    collection : Collection
        MongoDB collection ised to store task objects
    """
    def __init__(self, mongo_collection):
        """Intialize the MongoDB collection that stores task information.

        Parameters
        ----------
        mongo_collection : Collection
            Collection in MongoDB
        """
        self.collection = mongo_collection

    def create_task(self, workflow_id, node_id):
        """ Override TaskManager.create_task."""
        # Create a new entry in the workflow collection and return new instance
        self.collection.insert_one(
            {'workflow' : workflow_id, 'node' : node_id}
        )

    def delete_task(self, workflow_id, node_id):
        """Override TaskManager.delete_task."""
        # Delete all matching objects (in case duplicates have been entered)
        self.collection.delete_many({'workflow' : workflow_id, 'node' : node_id})

    def delete_tasks(self, workflow_id):
        """Override TaskManager.delete_task."""
        # Delete all matching objects
        self.collection.delete_many({'workflow' : workflow_id})

    def has_tasks(self, workflow_id):
        """Override TaskManager.delete_task."""
        return (self.collection.count({'workflow' : workflow_id}) > 0)

    def list_tasks(self, workflow_id):
        """Override TaskManager.list_tasks."""
        result = []
        # Build the document query. Select all entries if no state filter is
        # given.
        cursor = self.collection.find({'workflow' : workflow_id})
        # Iterate over all objects in the query result. Add task id to result.
        for document in cursor:
            result.append(document['node'])
        return result

    def list_workflows(self):
        """Override TaskManager.list_workflows"""
        return self.collection.find().distinct('workflow')
