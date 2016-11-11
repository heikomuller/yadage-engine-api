import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import yadageengine.api as api

web_ui = api.WebServiceAPI('http://my.host.com/yadage/////////')

print web_ui.urls.get_workflow_list_url()
print web_ui.urls.get_workflow_url('MyWorkflow')

web_ui = api.WebServiceAPI('http://my.host.com/yadage')

print web_ui.urls.get_workflow_list_url()
print web_ui.urls.get_workflow_url('MyWorkflow')

web_ui = api.WebServiceAPI('///')

web_ui = api.WebServiceAPI('http://my.host.com/yadage')
print web_ui.list_directory('../yadageengine', '')
