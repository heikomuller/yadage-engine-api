"""Factory for Web API resource Urls.


The Web API attempts to follow the Hypermedia As The Engine of Application State
(HATEOAS) constraint. Thus, every serialized resource contains a list of
references for clients to interact with the API. The URLFactory class in this
module contains all methods to generate HATEOAS references for API resources.
"""

# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------

# Json element name for HATEOAS reference lists
HATEOAS_LINKS = 'links'

# Url query parameter for workflow statuses
PARA_STATUS = 'status'


# ------------------------------------------------------------------------------
# Url factory
# ------------------------------------------------------------------------------

class UrlFactory:
    """Class that captures the definitions of Url's for any resource that is
    accessible through the Web API.

    Attributes
    ----------
    base_url : string
        Prefix for all resource Url's
    doc_url : string
        Url to API documentation
    """
    def __init__(self, base_url):
        """Intialize the common Url prefix and the reference to the API
        documentation.

        Parameters
        ----------
        base_url : string
            Prefix for all resource Url's
        doc_url : string
            Url to API documentation
        """
        self.base_url = base_url
        # Ensure that base_url does not end with a slash
        while len(self.base_url) > 0:
            if self.base_url[-1] == '/':
                self.base_url = self.base_url[:-1]
            else:
                break

    def file_url(self, path):
        """Url to download a workflow (output) file at the given path.

        Parameters
        ----------
        path : string
            Relative path to file resource in the Yadage engine workflow files
            folder on the shared file system.

        Returns
        -------
        string
            Url to file resource
        """
        return self.base_url + '/files/' + path

    def workflow_apply_rules_url(self, workflow_id):
        """Url for an apply rules request for a given workflow.

        Parameters
        ----------
        workflow_id : string
            Unique identifier of workflow resource

        Returns
        -------
        string
            Url to POST apply rules request
        """
        return self.workflow_url(workflow_id) + '/apply'

    def workflow_list_files_url(self, workflow_id):
        """Url to get a list of files on local disk that have been created by
        completed workflow steps.

        Parameters
        ----------
        workflow_id : string
            Unique identifier of workflow resource

        Returns
        -------
        string
            Url to GET workflow files listing
        """
        return self.workflow_url(workflow_id) + '/files'

    def workflow_list_url(self):
        """Url to retrieve workflow resources listing.

        Returns
        -------
        string
            Url for workflow resources listing.
        """
        return self.base_url + '/workflows'

    def workflow_stats_url(self):
        """Url to retrieve a summary of workflows by state.

        Returns
        -------
        string
            Url to retrieve workflows statistics object
        """
        return self.base_url + '/workflow-stats'

    def workflow_status_url(self, status):
        """Url to retrieve workflow resources listing containing only those
        workflows of a given status.

        Returns
        -------
        string
            Url for workflow resources listing.
        """
        return self.workflow_list_url() + '?' + PARA_STATUS + '=' + status

    def workflow_submit_nodes_url(self, workflow_id):
        """Url for an submit nodes request for a given workflow.

        Parameters
        ----------
        workflow_id : string
            Unique identifier of workflow resource

        Returns
        -------
        string
            Url to POST submit rules request
        """
        return self.workflow_url(workflow_id) + '/submit'

    def workflow_url(self, workflow_id):
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
        return self.workflow_list_url() + '/' + str(workflow_id)


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def hateoas_reference(rel, href):
    """Get HATEOAS reference object containing the Url 'href' and the link
    relation 'rel' that defines the type of the link.

    Parameters
    ----------
    rel : string
        Descriptive attribute defining the link relation
    href : string
        Http Url

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return {'rel' : rel, 'href' : href}


def self_reference(url):
    """Get HATEOAS self reference for a API resources.

    Parameters
    ----------
    url : string
        Url to resource

    Returns
    -------
    dict
        Dictionary containing elements 'rel' and 'href'
    """
    return hateoas_reference('self', url)
