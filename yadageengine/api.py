"""Collection of helper methods and classes for the Yadage Engine Web API
implementation.

The UrlFactory class wraps all definitions of API resource Url's. An instance
of this factory is used by the Web API to create resource Url's for HATEOAS
references.

The WebServiceAPI implements generic methods to create and manipulate objects
(API resources) that are managed by the Yadage Engine.
"""

import os


# ------------------------------------------------------------------------------
#
# Url's
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Urls' for resources following the HATEOAS (Hypermedia As The Engine Of
# Application State)  constraint of the REST application architecture.
# ------------------------------------------------------------------------------

""" HATEOAS relationship identifier"""
# Self reference
REF_SELF = 'self'


def add_self_reference(json_obj, url):
    """Add list of references to a Json object containing as the sole item a
    self reference.

    Parameters
    ----------
    json_obj : Json object
        Json object that is going to be modified by adding a list of HATEOAS
        references (containing a single self reference)
    url : string
        Url to resource

    Returns
    -------
    Json object
        Modified version of the given json_obj
    """
    json_obj['links'] = get_references({REF_SELF : url})
    return json_obj


def get_references(dictionary):
    """Generate a HATEOAS reference listing from a dictionary. Keys in the dictionary
    define relationships ('rel') and associated values are URL's ('href').

    Parameters
    ----------
    dictionary : Dictionary
        Dictionary of key value pairs where keys are HATEOAS reference
        descriptors and values are Url's associated with the key.

    Returns
    -------
    List
        List of dictionaries containing 'rel' and 'href' elements
    """
    links = []
    for key in dictionary:
        links.append({'rel' : key, 'href' : dictionary[key]})
    return links


# ------------------------------------------------------------------------------
# Classes
# ------------------------------------------------------------------------------
class URLFactory:
    """Class that captures the definitions of Url's for any resource that is
    accessible through the Web API.

    Attributes
    ----------
    base_url : string
        Prefix for all resource Url's
    """
    def __init__(self, base_url):
        """Intialize the common Url prefix.

        Parameters
        ----------
        base_url : string
            Prefix for all resource Url's
        """
        self.base_url = base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    # --------------------------------------------------------------------------
    # URL to download workflow files
    #
    # path::string
    #
    # returns string
    # --------------------------------------------------------------------------
    def url_file(self, path):
        """Url to download a workflow (output) file at the given path.

        Parameters
        ----------
        path : string
            Relative path to file resource in the yadage engine workflow files
            folder on the shared file system.

        Returns
        -------
        string
            Url to file resource
        """
        return self.base_url + '/files/' + path

    def get_workflow_url(self, workflow_id):
        """Url for workflow resource with given identifier.

        Parameters
        ----------
        workflow_id : string
            Unique identifier of workflow resource

        Returns
        -------
        string
            Url to workflow resource
        """
        return self.get_workflow_list_url() + '/' + str(workflow_id)

    def get_workflow_list_url(self):
        """Url to retrieve workflow resources listing.

        Returns
        -------
        string
            Url for workflow resources listing.
        """
        return self.base_url + '/workflows'


# ------------------------------------------------------------------------------
#
# YADAGE Web Service API
#
# ------------------------------------------------------------------------------
class WebServiceAPI(object):
    """Implements generic methods to create and manipulate objects
    (API resources) that are managed by the Yadage Engine.

    Attributes
    ----------
    urls : UrlFactory
        Factory for Web API resource Url's
    """
    def __init__(self, base_url):
        """Initialize the Web service Url factory.

        Parameters
        ----------
        base_url : string
            Prefix for all Url's to access resources on the Web API server.
        """
        self. urls = URLFactory(base_url)

    def list_directory(self, directory_name, relative_path):
        """Recursive listing of all files in the given directory.

        Parameters
        ----------
        directory_name : string
            Absolute path to directory that is being listed

            relative_path : string
                Path prefix of directory_name relative to a base directory that
                contains all workflow data files.

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
                    'files' : self.list_directory(abs_path, relative_path + '/' + filename)
                }
            else:
                descriptor = {
                    'type': 'FILE',
                    'name': filename,
                    'size': os.stat(abs_path).st_size,
                    'href': self.urls.url_file(relative_path + '/' + filename)
                }
            files.append(descriptor)
        return files

    def workflow_files(self, base_directory, workflow_id):
        """Get recursive directory listing for workflow.

        Parameters
        ----------
        base_directory : string
            Absolute path to base directory that contains all workflows data
            files (on the shared drive)
        workflow_id : string
            Unique identifier of workflow for which the files and file structure
            is returned.

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
        return {'files' : self.list_directory(base_directory, workflow_id)}

    def workflow_to_json(self, workflow):
        """Serialize a workflow instance as Json object.

        Parameters
        ----------
        workflow : workflow.WorkflowInstance
            Workflow instance object

        Returns
        -------
        Json object
            Json serialization of workflow instance
        """
        wf_descriptor = {
            'id' : workflow.identifier,
            'name' : workflow.name,
            'state' : workflow.state.name,
            'dag' : workflow.dag,
            'rules' : workflow.rules,
            'applied' : workflow.applied_rules,
            'applicableRules' : workflow.applicable_rules,
            'submittableNodes' : workflow.submittable_nodes,
            'stepsbystage' : workflow.stepsbystage,
            'bookkeeping' : workflow.bookkeeping
        }
        return add_self_reference(wf_descriptor, self.urls.get_workflow_url(workflow.identifier))

    def workflow_descriptor_to_json(self, workflow):
        """Serialize workflow descriptor as Json object.

        Parameters
        ----------
        workflow workflow.WorkflowDescriptor
            Workflow descriptor object

        Results
        -------
        Json object
            Json serialization of workflow descriptor
        """
        wf_descriptor = {
            'id' : workflow.identifier,
            'name' : workflow.name,
            'state' : workflow.state.name
        }
        return add_self_reference(wf_descriptor, self.urls.get_workflow_url(workflow.identifier))

    def workflow_descriptors_to_json(self, workflows):
        """Convert a list of workflow descriptors into a Json array.

        Parameters
        ----------
        workflows : List(workflow.WorkflowDescriptor)
            List of workflow descriptors

        Returns
        -------
        Json object
             [{id:..., name:..., state:...}]
        """
        descriptors = []
        for wf in workflows:
            descriptors.append(self.workflow_descriptor_to_json(wf))
        return {'workflows' : descriptors}
