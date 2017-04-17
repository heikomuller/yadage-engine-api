"""YADAGE Server Engine

Contains classes and methods for the main YADAGE workflow instance manager
that stores workflows and orchestrates their execution.
"""

from abc import abstractmethod
import adage
import adage.dagstate as dagstate
import adage.nodestate as nodestate
import os
import shutil
import threading
import uuid
import yadage.backends.celeryapp
from yadage.yadagemodels import YadageWorkflow

from workflow import WorkflowInstance
from workflow import WORKFLOW_RUNNING, WORKFLOW_WAITING, WORKFLOW_FAILED, WORKFLOW_SUCCESS


# ------------------------------------------------------------------------------
#
# Yadage Engine
#
# ------------------------------------------------------------------------------

class YADAGEEngine:
    """ YADAGE workflow engine used to manage and manipulate workflows. The
    engine is a wrapper around a workflow repository that store workflow
    instances, the Yadage backend used for task execution, and a task manager
    that maintains information about submitted tasks.

    The state of nodes (and therefore workflows) in the repository does not
    get automatically updated when a task finishes. Instead, whenever a
    workflow instance is accessed, the engine first checks whether any submitted
    task for the workflow have finished and updates the workflow state in the
    repository.
    """
    def __init__(self, workflow_db, yadage_backend, task_manager, backend_proxy_cls, work_dir):
        """Initialize workflow engine. Provide a workflow manager and the
        path to the global work directory shared by all workflows.

        Parameters
        ----------
        workflow_db : workflow.WorkflowRepository
            Workflow repository manager
        yadage_backend : yadage.backends.AdagePacktivityBackendBase
            Yadage backend for workflow execution
        task_manager : tasks.taskManager
            Task manager for submitted and running tasks
        backend_proxy_cls : *yadage.packtivitybackend.AdagePacktivityBackendBase
            Reference to class of task proxies used when instantiating Yadage
            workflows.
        work_dir : string
            Path to global work directory for workflows.
        """
        self.db = workflow_db
        self.backend = yadage_backend
        self.task_manager = task_manager
        self.backend_proxy_cls = backend_proxy_cls
        self.work_dir = os.path.abspath(work_dir)
        # Set the lock object for the manager
        self.lock = threading.Lock()

    def apply_rules(self, workflow_id, rule_instances):
        """Apply a given set of rule instances to the specified workflow
        instance using the default backend.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        rule_instances : List(string)
            List of rule identifier

        Returns
        -------
        Boolean
            True, if all rules where applied successfully.
        """
        # Use locking to ensure proper concurrency handling.
        with self.lock:
            # Get the Yadage workflow and workflow instance from the repository.
            # Return None if workflow does not exist.
            yadage_workflow, workflow_inst = self.get_current_workflow_objects(workflow_id)
            if workflow_inst is None:
                return False
            # Apply each rule in the given rule instance set. Return False if
            # one of the rules could not be applied (i.e., does not exist).
            for rule_id in rule_instances:
                # Find the rule that is referenced by the rule instance. Keep
                # track of the rule index so we can remove it from the rule
                # list. Return False if rule not found.
                ref_rule = None
                rule_index = 0
                for rule in yadage_workflow.rules:
                    if rule.identifier == rule_id:
                        ref_rule = rule
                        break
                    rule_index += 1
                if ref_rule is None:
                    return False
                # Remove the rule from the list of rules in the workflow and
                # apply the rule. Make sure to add the applied rules to the list
                # of applied rules in the workflow.
                del yadage_workflow.rules[rule_index]
                ref_rule.apply(yadage_workflow)
                yadage_workflow.applied_rules.append(ref_rule)
            # Update workflow in the repository. Note that state should not
            # have changed by just applying rules.
            self.db.update_workflow(
                workflow_id,
                workflow_inst.name,
                workflow_inst.state,
                yadage_workflow.json()
            )
            return True

    def create_workflow(self, workflow_def, name, init_data={}):
        """Create a new workflow instance.

        Parameters
        ----------
        workflow_def : Json object
            Json object representing workflow template
        name : string
            User-provided name of the workflow
        init_data : Dictionary
            Dictionary of user-provided arguments for workflow instantiation

        Returns
        -------
        workflow.WorkflowDescriptor
            Descriptor for created workflow
        """
        # Generate a unique identifier for the new workflow instance
        identifier = str(uuid.uuid4())
        # Create root context for new workflow instance. Will create a new
        # directory in the work base directory with the workflow identifier as
        # directory name
        workdir = os.path.join(self.work_dir, identifier)
        os.makedirs(workdir)
        root_context = {
            'readwrite': [workdir],
            'readonly': []
        }
        # Create YADAGE Workflow from the given workflow template. Initialize
        # workflow with optional inittialization parameters.
        workflow = YadageWorkflow.createFromJSON(workflow_def, root_context)
        workflow.view().init(init_data)
        workflow_json = workflow.json()
        # The initial workflow state is WAITING. Change this if we allow auto-
        # apply and submit.
        state = WORKFLOW_WAITING
        # Store workflow in the associated instance database and return the
        # workflow descriptor
        return self.db.create_workflow(
            identifier,
            name,
            state,
            workflow_json
        )

    def delete_workflow(self, workflow_id):
        """Delete workflow with the given identifier. If workflow existed,
        remove all files associated with the workflow from the shared file
        system.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        Boolean
            True, if worlflow deleted, False if not found.
        """
        # Use locking avoid deleting a workflow that is currently being
        # manipulated by another thread.
        with self.lock:
            if self.db.delete_workflow(workflow_id):
                # Delete tasks associated with the workflow
                self.task_manager.delete_tasks(workflow_id)
                # Delete all files associated with the workflow
                workdir = os.path.join(self.work_dir, workflow_id)
                shutil.rmtree(workdir)
                return True
            else:
                return False

    def get_current_workflow_objects(self, workflow_id):
        """Retrieve Yadage workflow and workflow instance object from the
        repository. Ensure that workflow state is up to date. This method may
        modify the workflow instance in the repository. Assumes that the
        calling thread holds the lock on the Yadage engine to ensure proper
        behaviour in case of concurrent access.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        yadage.yadagemodels.YadageWorkflow. workflow.WorkflowInstance
            Tuple of Yadage workflow object and workflow instance object. The
            result is (None, None), if no workflow with given identifier exists
            in the repository.
        """
        # Retrieve the unmodified workflow objects from the repository. Return
        # None if either is None.
        yadage_workflow, workflow_inst = self.get_workflow_objects(workflow_id)
        if yadage_workflow is None or workflow_inst is None:
            return None, None
        # Check if the workflow has any submittted tasks, If yes, update the
        # workflow state.
        if self.task_manager.has_tasks(workflow_id):
            return self.update_workflow_state(yadage_workflow, workflow_inst)
        else:
            return yadage_workflow, workflow_inst

    def get_workflow(self, workflow_id):
        """Get workflow with given identifier.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        workflow.WorkflowInstance
            Workflow instance object. Result is None if workflow does not exist.
        """
        # Use lock to ensure that concurrency is handled in case the workflow
        # state needs to be updated.
        with self.lock:
            # Get the workflow instance from the database. Return None if it
            # does not exist.
            yadage_workflow, workflow_inst = self.get_current_workflow_objects(workflow_id)
            if workflow_inst is None:
                return None
        # Get the list of identifier for rules that are applicable.
        applicable_rules = []
        for rule in reversed([x for x in yadage_workflow.rules]):
            if rule.applicable(yadage_workflow):
                applicable_rules.append(rule.identifier)
        # Get list of identifier for submittable nodes
        submittable_nodes = []
        for node in self.submittable_nodes(yadage_workflow, workflow_id):
            submittable_nodes.append(node.identifier)
        # Return a full workflow instance
        return WorkflowInstance(
            workflow_inst.identifier,
            workflow_inst.name,
            workflow_inst.state,
            workflow_inst.workflow_json['dag'],
            workflow_inst.workflow_json['rules'],
            workflow_inst.workflow_json['applied'],
            applicable_rules,
            submittable_nodes,
            workflow_inst.workflow_json['stepsbystage'],
            workflow_inst.workflow_json['bookkeeping']
        )

    def get_workflow_objects(self, workflow_id):
        """Get workflow instance for given identifier. Returns a pair of Yadage
        workflow and workflow instance. This is a read-only operation and
        therefore no lock is aquired.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        yadage.yadagemodels.YadageWorkflow. workflow.WorkflowInstance
            Tuple of Yadage workflow object and workflow instance object. The
            result is (None, None), if no workflow with given identifier exists
            in the repository.
        """
        # Get the workflow instance from the database. Return None if it does
        # not exist. Otherwise, call implementation specific instantiation
        # method.
        workflow_inst = self.db.get_workflow(workflow_id)
        if not workflow_inst is None:
            yadage.backends.celeryapp.app.set_current()
            return YadageWorkflow.fromJSON(
                workflow_inst.workflow_json,
                self.backend_proxy_cls,
                backend=self.backend
            ), workflow_inst
        else:
            return None, None

    @staticmethod
    def get_workflow_state(workflow):
        """Derive the state of the workflow from the state of the nodes in the
        workflow DAG and the set of workflow rules.

        Parameters
        ----------
        workflow : yadage.yadagemodels.YadageWorkflow

        Returns
        -------
        WorkflowState
            State object for given workflow
        """
        dag = workflow.dag
        rules = workflow.rules
        # If there are failed nodes the workflow satet is failed. If there are
        # running nodes then the state is running. Otherwiese, the workflow
        # is idele if there are applicab;e nodes or submittable tasks.
        state = WORKFLOW_WAITING if len(workflow.rules) > 0 else WORKFLOW_SUCCESS
        for node_id in dag.nodes():
            node = dag.getNode(node_id)
            if node.state == nodestate.FAILED:
                state = WORKFLOW_FAILED
                break
            if node.state == nodestate.RUNNING:
                state = WORKFLOW_RUNNING
            if node.state == nodestate.DEFINED:
                if state != WORKFLOW_FAILED and state != WORKFLOW_RUNNING:
                    state = WORKFLOW_WAITING
        return state

    def list_workflows(self):
        """Get a list of all workflows currently managed by the engine.

        Returns
        -------
        List(workflow.WorkflowDescriptor)
            List of descriptors for all workflows in the repository.
        """
        with self.lock:
            # Update the state of all workflows that have running tasks
            for workflow_id in self.task_manager.list_workflows():
                yadage_workflow, workflow_inst = self.get_workflow_objects(workflow_id)
                if not yadage_workflow is None and not workflow_inst is None:
                    self.update_workflow_state(yadage_workflow, workflow_inst)
                else:
                    self.task_manager.delete_tasks(workflow_id)
        return self.db.list_workflows()

    def submit_nodes(self, workflow_id, node_ids):
        """Submit a set of tasks (referenced by their node identifier) for the
        specified workflow instance using the default backend.

        workflow_id : string
            Unique workflow identifier
        node_ids : List(string)

        Returns
        -------
        Boolean
            True, if all tasks where submitted successfully
        """
        # Use locking to ensure proper concurrency handling.
        with self.lock:
            # Get the workflow instance from the database. Return False if it
            # does not exist.
            yadage_workflow, workflow_inst = self.get_current_workflow_objects(workflow_id)
            if workflow_inst is None:
                return False
            # Filter submitted nodes from set of submittable nodes
            nodes = []
            for node in self.submittable_nodes(yadage_workflow, workflow_id):
                if node.identifier in node_ids:
                    nodes.append(node)
            # TODO: What should happen if unknown nodes are encountered?
            if len(nodes) != len(node_ids):
                return False
            # Create an entry for each node in the task repository before
            # submitting to the backend for execution
            for node in nodes:
                adage.submit_node(node, self.backend)
                self.task_manager.create_task(workflow_id, node.identifier)
            # Get the state of the workflow and update the workflow in the database.
            state = self.get_workflow_state(yadage_workflow)
            self.db.update_workflow(
                workflow_id,
                workflow_inst.name,
                state,
                yadage_workflow.json()
            )
            return True

    def submittable_nodes(self, workflow, workflow_id):
        """Get a list of all node objects that are submittable in the given
        workflow.

        Parameters
        ----------
        workflow : yadage.yadagemodels.YadageWorkflow
            Yadage workflow object
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        List(Node)
            List of node objects
        """
        # Get list of pending tasks to avoid including these tasks in the list
        # of submittable nodes
        pending_nodes = self.task_manager.list_tasks(workflow_id)
        # Get list of nodes that (1) are not pending tasks, (2) have not been
        # submitted yet, and (3) have all upstream dependencies fulfilled.
        nodes = []
        for node_id in workflow.dag.nodes():
            if node_id in pending_nodes:
                continue
            node = workflow.dag.getNode(node_id)
            if node.submit_time:
                continue;
            if dagstate.upstream_ok(workflow.dag, node):
                nodes.append(node)
        return nodes

    def update_workflow_state(self, yadage_workflow, workflow_inst):
        """Update the state of a given workflow in the repository. Start by
        retrieving the list of all submitted and running nodes for the workflow.
        Check the state for each of these nodes. If either node is no longer
        running, update workflow in repository.

        This method assumes that the calling method has the lock on the Yadage
        Engine to avoid unwanted side effect due to concurrent access.

        Parameters
        ----------
        yadage_workflow : yadage.yadagemodels.YadageWorkflow
            Yadage workflow object
        workflow_inst :  workflow.WorkflowInstance
            Workflow  instance object in repository

        Returns
        -------
        yadage.yadagemodels.YadageWorkflow. workflow.WorkflowInstance
            Tuple of Yadage workflow object and workflow instance object. The
            result is (None, None), if no workflow with given identifier exists
            in the repository.
        """
        # The modified workflow instance. Initially set to the current instance
        modified_workflow_inst = workflow_inst
        # Get the list of tasks for the given workflow.
        tasks = self.task_manager.list_tasks(workflow_inst.identifier)
        if len(tasks) > 0:
            # Keep track if any submitted task has finished
            has_changes = False
            for task_id in tasks:
                node = yadage_workflow.dag.getNode(task_id)
                if not node is None:
                    if node.state in[nodestate.SUCCESS, nodestate.FAILED]:
                        # Node state must have changed. Remove task from repository
                        self.task_manager.delete_task(workflow_inst.identifier, task_id)
                        has_changes = True
                else:
                    # Delete entry for non-existing node
                    db.task_manager.delete_task(workflow_inst.identifier, task_id)
            # Only need to update the workflow instance in the repository if at
            # least one of the nodes changed state
            if has_changes:
                state = self.get_workflow_state(yadage_workflow)
                modified_workflow_inst = self.db.update_workflow(
                    workflow_inst.identifier,
                    workflow_inst.name,
                    state,
                    yadage_workflow.json()
                )
        return yadage_workflow, modified_workflow_inst
