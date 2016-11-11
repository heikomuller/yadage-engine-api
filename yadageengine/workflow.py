"""Workflow objects managed by the YADAGE Engine and stored in the workflow
respository."""


# ------------------------------------------------------------------------------
#
# Workflow States
#
# -----------------------------------------------------------------------------

class WorkflowState(object):
    """Representation of a workflow state. At this point the state is simple
    represented by a unique descriptive name.

    Attributes
    ----------
    name : string
        Text representation of workflow state
    """
    def __init__(self, name):
        """Initialize the name of a workflow state.

        Parameters
        ----------
        name : string
            Text representation of workflow state
        """
        self.name = name

    def __repr__(self):
        """Unambiguous printable representation of the workflow state.

        Returns
        -------
        string
        """
        return '<WorkflowState: {}>'.format(self.name)

    def __str__(self):
        """Readable printable representation of the workflow state.

        Returns
        -------
        string
        """
        return self.name


# ------------------------------------------------------------------------------
# Workflow State Instances
# ------------------------------------------------------------------------------

"""Define possible workflow states. A workflow can be in either of four
different states: RUNNING (i.e., submitted task that is running), WAITING
(i.e., waiting for user interaction), FAILED (i.e., execution of at least on
task falied), and SUCCESS (i.e., workflow successfully completed).
"""
WORKFLOW_RUNNING = WorkflowState('RUNNING')
WORKFLOW_WAITING = WorkflowState('WAITING')
WORKFLOW_FAILED  = WorkflowState('FAILED')
WORKFLOW_SUCCESS = WorkflowState('SUCCESS')


# ------------------------------------------------------------------------------
# Dictionary of workflow states
# ------------------------------------------------------------------------------

WORKFLOW_STATES = {
    'WAITING': WORKFLOW_WAITING,
    'RUNNING': WORKFLOW_RUNNING,
    'FAILED':  WORKFLOW_FAILED,
    'SUCCESS': WORKFLOW_SUCCESS,
}

# ------------------------------------------------------------------------------
#
# Workflow Descriptor
#
# ------------------------------------------------------------------------------

class WorkflowDescriptor(object):
    """ Workflow descriptors maintain basic information about workflow instances
    managed by the YADAGE server. This information includes the unique workflow
    identifier, the descritpive (user-provided) workflow name, and the current
    state of the workflow. More comprehensive workflow classes will inherit from
    this base class.

    While the state of a workflow can be derived from the workflow Json object,
    storing it separately is intended to speed up the generation of workflow
    listing containing each workflow's state in the Web API. It is expected that
    the engine using the instance manager ensures that the state that is stored
    with the instance is identical to the state that would be derived from the
    Json object.

    Attributes
    ----------
    identifier : string
        Unique workflow identifier
    name : string
        User-defined workflow name
    state : WorkflowState
        Workflow state object
    """
    def __init__(self, identifier, name, state):
        """Initialize the identfifier, name, and state of the workflow
        descriptor.

        Parameters
        ----------
        identifier : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : WorkflowState
            Workflow state object
        """
        self.identifier = identifier
        self.name = name
        self.state = state


# ------------------------------------------------------------------------------
#
# Workflow Instances
#
# ------------------------------------------------------------------------------

class WorkflowDBInstance(WorkflowDescriptor):
    """Workflow instance that is managed by a workflow instance manager. Extends
    the workflow descriptor with the YADAGE Json representation of the workflow.

    Attributes
    ----------
    identifier : string
        Unique workflow identifier
    name : string
        User-defined workflow name
    state : WorkflowState
        Workflow state object
    workflow_json : Json object
        Json object for Yadage workflow instance
    """
    def __init__(self, identifier, name, state, workflow_json):
        """Initialize the identfifier, name, state, and Json object containing
        the YADAGE workflow representation.

        Parameters
        ----------
        identifier : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : WorkflowState
            Workflow state object
        workflow_json : Json object
            Json object for Yadage workflow instance
        """
        super(WorkflowDBInstance, self).__init__(identifier, name, state)
        self.workflow_json = workflow_json


class WorkflowInstance(WorkflowDescriptor):
    """Full workflow instance object. Extends the workflow descriptor with the
    workflow graph, list of rules, list of applied rules, and list of applicable
    rule identifier.

    Attributes
    ----------
    identifier : string
        Unique workflow identifier
    name : string
        User-defined workflow name
    state : WorkflowState
        Workflow state object
    dag : Json object
        Json serialization of Adage DAG
    rules : Json object
        Json serialization of workflow rules
    applied_rules Json object
        Json serialization of applied rules
    applicable_rules : List(string)
        List of applicable rules identifier
    submittable_nodes : List(string)
        List of submittable nodes identifier
    """
    def __init__(self, identifier, name, state, dag, rules, applied_rules, applicable_rules, submittable_nodes):
        """Initialize the identfifier, name, state, dag, rules, applied rules
        and applicable rule identifier. At this stage all ADAGE objects are
        simply Json objects.

        Parameters
        ----------
        identifier : string
            Unique workflow identifier
        name : string
            User-defined workflow name
        state : WorkflowState
            Workflow state object
        dag : Json object
            Json serialization of Adage DAG
        rules : Json object
            Json serialization of workflow rules
        applied_rules Json object
            Json serialization of applied rules
        applicable_rules : List(string)
            List of applicable rules identifier
        submittable_nodes : List(string)
            List of submittable nodes identifier
        """
        super(WorkflowInstance, self).__init__(identifier, name, state)
        self.dag = dag
        self.rules = rules
        self.applied_rules = applied_rules
        self.applicable_rules = applicable_rules
        self.submittable_nodes = submittable_nodes
