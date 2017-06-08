"""Yadage Workflow Engine API

The engine API is a wrapper around the different components that are necessary
to implement a persistent workflow execution engine.

The workflow repository is used as persistent storage backend for workflow
instances. The default implementation of the engine API uses MongoDB as
storage backend.

The engine returns serialized resources, i.e., dictionaries.
"""

import json
import os
import shutil
import urllib

from hateoas import UrlFactory, self_reference, hateoas_reference, HATEOAS_LINKS
from workflow import WorkflowRepository


# ------------------------------------------------------------------------------
#
# Yadage Engine
#
# ------------------------------------------------------------------------------

class YADAGEEngine(object):
    """ YADAGE workflow engine is used to manage and manipulate workflows. The
    engine is a wrapper around a workflow repository that stores workflow
    instances, the Yadage backend used for task execution, and a task manager
    that maintains information about submitted tasks.

    Attributes
    ----------
    db : workflow.WorkflowRepository
        Repository of managed workflows
    description : dict
        Serialization of the Web service description
    urls : hateoas.URLFactory
        Factory for resource urls
    workflow_dir : string
        Base directory for all workflow files
    """
    def __init__(self, config):
        """Initialize the API from a given configuration object. Expects the
        following configuration properties:

        * server.apppath : Application path part of the Url to access the app
        * server.url : Base Url of the server where the app is running
        * server.port: Port the server is running on
        * app.doc : Url to web service documentation
        * db.workdir : Path to local directory for workflow files
        * mongo.db : Name of MongoDB database containing workflow state information
        * mongo.uri (optional): Uri containing MongoDB host and port
        """
        # Initialize the workflow repository
        self.db = WorkflowRepository(config)
        # Initialize the Url Factory with the application Url
        base_url = config['server.url']
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        server_port = config['server.port']
        if server_port != 80:
            base_url += ':' + str(server_port)
        base_url += config['server.apppath'] + '/'
        self.urls = UrlFactory(base_url)

        # Base directory for all workflow files. Create the directory if it does
        # not exist.
        self.workflow_dir = os.path.abspath(config['db.workdir'])
        if not os.access(self.workflow_dir, os.F_OK):
            os.makedirs(self.workflow_dir)

        # The workflows listing Url is also used as Url for submitting workflow
        # run requests
        action_url =  self.urls.workflow_list_url()
        self.description = {
            'name': config['app.name'],
            HATEOAS_LINKS : [
                self_reference(self.urls.base_url),
                hateoas_reference('doc', config['app.doc']),
                hateoas_reference('workflows.list', action_url),
                hateoas_reference('workflows.submit', action_url)
            ]
        }

    def apply_rules(self, workflow_id, rule_instances):
        """Apply a given set of rule instances to the specified workflow
        instance.

        Raises ValueError if any of the elected rules is not applicable. The
        result is None if the given workflow does not exist.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        rule_instances : list(string)
            List of rule identifiers

        Returns
        -------
        dict
            Workflow instance or None
        """
        # Get the workflow object. Return None if workflow does not exist.
        workflow = self.db.get_workflow(workflow_id)
        if workflow is None:
            return None
        # Apply selected rules. Will throw ValueError if any of the given rules
        # is not applicable
        workflow.apply_rules(rule_instances)
        # Reload workflow object to get updated program state
        return self.get_workflow(workflow_id)

    def create_workflow(self, workflow_template_url, parameters={}, name=None):
        """Create a new workflow instance from the given workflow template.

        Raises a ValueError if the provided workflow parameter set is invalid.

        Parameters
        ----------
        workflow_template_url : string
            Url for workflow template
        parameters,optional : Dictionary
            Dictionary of user-provided arguments for workflow instantiation
        name : string, optional
            User-provided name of the workflow. If no name is provided the
            template name is used as workflow name.

        Returns
        -------
        dict
            Workflow instance
        """
        # Read the template at the given template URL
        workflow_def = json.loads(urllib.urlopen(workflow_template_url).read())
        # Construct dictionary of input data
        init_data = {}
        if 'parameters' in workflow_def:
            for para in workflow_def['parameters']:
                para_key = para['name']
                para_value = None
                if para_key in parameters:
                    para_value = parameters[para_key]
                elif 'default' in para:
                    para_value = para['default']
                else:
                    raise ValueError('missing value for parameter: ' + para_key)
                # TODO: Convert parameter values from strings to requested type
                if para['type'] == 'int':
                    para_value = int(para_value)
                elif para['type'] == 'float':
                    para_value = float(para_value)
                elif para['type'] == 'array':
                    value_list = para_value.split(',')
                    para_value = []
                    for val in value_list:
                        if para['items'] == 'int':
                            para_value.append(int(val))
                        elif para['type'] == 'float':
                            para_value.append(float(val))
                        elif para['type'] == 'string':
                            para_value.append(val)
                        else:
                            raise ValueError('missing value for list item: ' + val)
                elif para['type'] != 'string':
                    raise ValueError('unknown parameter type: ' + para['type'])
                init_data[para_key] = para_value
        # Use template name if no workflow name was provided or the give name
        # is empty
        workflow_name = name
        if not workflow_name is None:
            if workflow_name == '':
                workflow_name = str(workflow_def['name'])
        else:
            workflow_name = str(workflow_def['name'])
        # Create the workflow and return descriptor
        return serialize_workflow(
            self.db.create_workflow(
                workflow_def['schema'],
                workflow_name,
                init_data
            ),
            self.urls
        )

    def delete_workflow(self, workflow_id):
        """Delete workflow with the given identifier. Removes workflow from
        workflow repository as well as all files associated with the workflow.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        Boolean
            True, if worlflow was deleted, False if not found.
        """
        # Remove workflow directory if workflow exists
        if self.db.delete_workflow(workflow_id):
            shutil.rmtree(os.path.join(self.workflow_dir, workflow_id))
            return True
        else:
            return False

    def get_description(self):
        """Descriptive object for Web API. Contains the API name and a list of
        references to list workflows and to submit new workflows. Also contains
        a references to the API documentation.

        Returns
        -------
        dict
            Dictionary containing API name and list of HATEOAS references
        """
        return self.description

    def get_workflow(self, workflow_id):
        """Get workflow instance with the given identifier.

        Result is None if the workflow does not exist.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier

        Returns
        -------
        dict
            Workflow instance object or None
        """
        return serialize_workflow(self.db.get_workflow(workflow_id), self.urls)

    def list_workflows(self):
        """Get a list of all workflows currently managed by the engine.

        Returns
        -------
        dict
            Listing of workflow descriptors
        """
        return {
            'workflows': [
                serialize_workflow_descriptor(wf, self.urls)
                    for wf in self.db.list_workflows()
            ],
            HATEOAS_LINKS: [
                self_reference(self.urls.workflow_list_url())
            ]
        }

    def list_workflow_files(self, workflow_id):
        """Get recursive directory listing for workflow.

        Result is None if workflow does not exist.

        Parameters
        ----------
        workflow_id : string
            Unique identifier of workflow for which the files and file structure
            is returned.

        Returns
        -------
        dict
            Listing of workflow files or None
        """
        return {
            'files' : list_directory(
                os.path.join(self.workflow_dir, workflow_id),
                workflow_id,
                self.urls
            )
        }

    def submit_nodes(self, workflow_id, node_ids):
        """Submit a set of tasks (referenced by their node identifier) for the
        specified workflow instance using the default backend.

        Result is None if workflow does not exist. Raises ValueError if any of
        the selected nodes is not submittable.

        Parameters
        ----------
        workflow_id : string
            Unique workflow identifier
        node_ids : list(string)
            List of identifier for nodes to be submitted

        Returns
        -------
        dict
            Workflow instance or None
        """
        # Get the workflow object. Return None if workflow does not exist.
        workflow = self.db.get_workflow(workflow_id)
        if workflow is None:
            return None
        # Apply selected rules. Will throw ValueError if any of the given rules
        # is not applicable
        workflow.submit_nodes(node_ids)
        # Reload workflow object to get updated program state
        return self.get_workflow(workflow_id)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def list_directory(directory_name, relative_path, urls):
    """Recursive listing of all files in the given directory.

    Parameters
    ----------
    directory_name : string
        Absolute path to directory that is being listed
    relative_path : string
        Path prefix of directory_name relative to a base directory that
        contains all workflow data files.
    urls : hateoas.UrlFactory
        Factory for resource urls

    Returns
    -------
    Dictionary
        Recursive directory listing {'files':[{file}]}:
        file : {
            'type', : 'DIRECTORY',
            'name': ...,
            'files' : [{file}]
        }
        or
        file : {
            'type': 'FILE',
            'name': ...,
            'size': ...,
            'href': ...
        }
    """
    files = []
    for filename in os.listdir(directory_name):
        abs_path = os.path.join(directory_name, filename)
        if os.path.isdir(abs_path):
            descriptor = {
                'type' : 'DIRECTORY',
                'name': filename,
                'files' : list_directory(abs_path, relative_path + '/' + filename, urls)
            }
        else:
            descriptor = {
                'type': 'FILE',
                'name': filename,
                'size': os.stat(abs_path).st_size,
                'href': urls.file_url(relative_path + '/' + filename)
            }
        files.append(descriptor)
    return files


def serialize_workflow(workflow, urls):
    """Serilaize workflow object. Contains all information about a workflow
    including the DAG, applicable rules, and submittable
    nodes.

    The list of references contains a self references, as well as references to
    delete the workflow ('delete'), list workflow files ('files'), apply rules
    ('apply'), submit nodes ('submit'), and to get a list of all worflows
    ('list').

    Returns None if workflow is not defined.

    Parameters
    ----------
    workflow : workflow.WorkflowInstance
        Workflow descriptor object
    urls : hateoas.UrlFactory
        Factory for resource urls

    Results
    -------
    dict
        Serialization of workflow object
    """
    # Ensure that workflow is defined
    if workflow is None:
        return None
    workflow_url = urls.workflow_url(workflow.identifier)
    return {
        'id' : workflow.identifier,
        'name' : workflow.name,
        'status' : workflow.status,
        'applicableRules' : workflow.applicable_rules,
        'submittableNodes' : workflow.submittable_nodes,
        'dag' : workflow.json()['dag'],
        'rules': workflow.json()['rules'],
        'appliedRules' : workflow.json()['applied'],
        HATEOAS_LINKS : [
            self_reference(workflow_url),
            hateoas_reference('delete', workflow_url),
            hateoas_reference(
                'files',
                urls.workflow_list_files_url(workflow.identifier)
            ),
            hateoas_reference(
                'apply',
                urls.workflow_apply_rules_url(workflow.identifier)
            ),
            hateoas_reference(
                'submit',
                urls.workflow_submit_nodes_url(workflow.identifier)
            ),
            hateoas_reference('list', urls.workflow_list_url()),
        ]
    }


def serialize_workflow_descriptor(workflow, urls):
    """Serilaize workflow descriptor. Returns a dictionary that contains the
    workflow id, name, state, and a reference to the workflow resource.

    Returns None if workflow is not defined.

    Parameters
    ----------
    workflow : workflow.WorkflowInstance
        Workflow descriptor object

    urls : hateoas.UrlFactory
        Factory for resource urls

    Results
    -------
    dict
        Serialization of workflow descriptor
    """
    # Ensure that workflow is defined
    if workflow is None:
        return None
    return {
        'id' : workflow.identifier,
        'name' : workflow.name,
        'status' : workflow.status,
        HATEOAS_LINKS : [
            self_reference(urls.workflow_url(workflow.identifier))
        ]
    }
