#!venv/bin/python

"""ADAGE - Web Server

The ADAGE Web API is the access point to the AS from the Web. The Web server
represents a thin layer around the ADAGE Engine that wraps all relevant
methods of the engine API.
"""

from flask import Flask, abort, jsonify, make_response, request, send_from_directory
from flask_cors import CORS
import getopt
import json
import os
from pymongo import MongoClient
import urllib
import sys

import yadage.backends.packtivity_celery
from yadage.backends.packtivity_celery import PacktivityCeleryBackend

from api import WebServiceAPI
from engine import YADAGEEngine
from task import MongoDBTaskManager
from repository import MongoDBWorkflowRepository


# ------------------------------------------------------------------------------
#
# App Configuration and Initialization
#
# ------------------------------------------------------------------------------

SERVER_URL = 'http://localhost'
PORT = 5006
APP_PATH = '/yadage-engine/api/v1'

# Base directory for all workflow files
WORK_BASE = '../../backend/data/'

# Directory for log files. The default is the shared file system base directory.
LOG_DIR = os.path.abspath(WORK_BASE)

# Switch debugging ON/OFF
DEBUG = False

# ------------------------------------------------------------------------------
# Parse command line arguments
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    command_line = """
    Usage:
    [-a | --path] <app-path>
    [-d | --debug]
    [-f | --files] <directory>
    [-l | --logs] <directory>
    [-p | --port] <port-number>
    [-s | --server] <server-url>
    """

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'a:df:l:p:s:', 'debug,files=,logs=,path=,port=0,server=')
    except getopt.GetoptError:
        print command_line
        sys.exit()

    if len(args) != 0:
        print command_line
        sys.exit()

    for opt, param in opts:
        if opt in ('-a', '--path'):
            APP_PATH = param
            if not APP_PATH.startswith('/'):
                print 'Invalid application path: ' + APP_PATH
                sys.exit()
            if APP_PATH.endswith('/'):
                APP_PATH = APP_PATH[:-1]
        elif opt in ('-d', '--debug'):
            DEBUG = True
        elif opt in ('-f', '--files'):
            WORK_BASE = param
        elif opt in ('-l', '--logs'):
            LOG_DIR = param
        elif opt in ('-p', '--port'):
            try:
                PORT = int(param)
            except ValueError:
                print 'Invalid port number: ' + param
                sys.exit()
        elif opt in ('-s', '--server'):
            SERVER_URL = param
            if SERVER_URL.endswith('/'):
                SERVER_URL = SERVER_URL[:-1]

# Make sure the directory for workflow inputs and outputs exists
if not os.access(WORK_BASE, os.F_OK):
    print 'Directory not found: ' + WORK_BASE
    sys.exit()
WORK_BASE = os.path.abspath(WORK_BASE)

# Make sure that the log directory exists (only if not in debug mode)
if not DEBUG:
    if not os.access(LOG_DIR, os.F_OK):
        print 'Directory not found: ' + LOG_DIR
        sys.exit()

# ------------------------------------------------------------------------------
# Initialize the Web app
# ------------------------------------------------------------------------------

# Create the app and enable cross-origin resource sharing
app = Flask(__name__, static_url_path=WORK_BASE)
app.config['APPLICATION_ROOT'] = APP_PATH
app.config['DEBUG'] = DEBUG
CORS(app)

# Initialize the URL factory. Base URL is used as prefix for all HATEOAS URL's
if PORT != 80:
    BASE_URL = SERVER_URL + ':' + str(PORT) + APP_PATH
else:
    BASE_URL = SERVER_URL + APP_PATH
api = WebServiceAPI(BASE_URL)
print 'Running at ' + BASE_URL

# ------------------------------------------------------------------------------
# Initialize the YADAGE Server engine.
# ------------------------------------------------------------------------------

# Here we use MongoDB as storage backend. Workflows are stored in database
#'yadage' and collection 'workflows'.
db = MongoClient().yadage
# Yadage backend
yadage_engine = YADAGEEngine(
    MongoDBWorkflowRepository(db.workflows),
    PacktivityCeleryBackend(yadage.backends.celeryapp.app),
    MongoDBTaskManager(db.tasks),
    yadage.backends.packtivity_celery.PacktivityCeleryProxy,
    WORK_BASE
)


# ------------------------------------------------------------------------------
#
# API
#
# ------------------------------------------------------------------------------

@app.route('/')
def get_welcome():
    """GET - Welcome Message

    Main object for the web service. Contains the service name and a list of
     references (including a reference to the API documentation, which is
     currently a hard coded URL).
    """
    return jsonify({
        'name': 'Workflow Engine API',
        'links' : [
            {'rel' : 'self', 'href' : BASE_URL},
            {'rel' : 'doc', 'href' : 'http://cds-swg1.cims.nyu.edu/yadage-engine/api/v1/doc/index.html'},
            {'rel' : 'workflows', 'href' : api.urls.get_workflow_list_url()}
        ]
    })


@app.route('/files/<path:path>')
def send_file(path):
    """GET - Workflow file

    Returns a file from a workflow working directory.
    """
    return send_from_directory(WORK_BASE, path)


@app.route('/workflows')
def list_workflows():
    """GET - Workflow Listing

    Returns a list of workflows currently managed by the workflow engine.
    """
    # Get list of worklows and return a list of workflow descriptors
    workflows = yadage_engine.list_workflows()
    return jsonify(api.workflow_descriptors_to_json(workflows))


@app.route('/workflows', methods=['POST'])
def create_workflow():
    """POST - Submit RECAST request

    Handles request to run a workflow template.
    """
    # Abort with BAD REQUEST if the request body is not in Json format or
    # does not contain a reference to a workflow template
    if not request.json:
        abort(400)
    if not 'template' in request.json:
        abort(400)
    # Get dictionary of user provided input data
    request_para = {}
    if 'parameters' in request.json:
        for para in request.json['parameters']:
            if not 'key' in para or not 'value' in para:
                abort(400)
            request_para[para['key']] = para['value']
    # Read the template at the given template URL
    template_url = request.json['template']
    workflow_def = json.loads(urllib.urlopen(template_url).read())
    # Construct dictionary of input data
    init_data = {}
    if 'parameters' in workflow_def:
        for para in workflow_def['parameters']:
            para_key = para['name']
            para_value = None
            if para_key in request_para:
                para_value = request_para[para_key]
            elif 'default' in para:
                para_value = para['default']
            else:
                abort(400)
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
                        abort(500)
            elif para['type'] != 'string':
                abort(500)
            init_data[para_key] = para_value
    # Get the user provided workflow name. If no name was provided use the
    # template name
    if 'name' in request.json:
        workflow_name = request.json['name'].strip()
        if workflow_name == '':
            workflow_name = str(workflow_def['name'])
    else:
        workflow_name = str(workflow_def['name'])
    # Submit request to workflow engine
    workflow = yadage_engine.create_workflow(workflow_def, workflow_name, init_data=init_data)
    #print workflow.identifier
    # Send response with code 201 (HTTP Created)
    return jsonify(api.workflow_descriptor_to_json(workflow)), 201


@app.route('/workflows/<string:workflow_id>')
def get_workflow(workflow_id):
    """GET - Workflow

    Retrieve workflow with given identifier.
    """
    # Get workflow from engine. Abort with NOT FOUND (404) if result is null.
    workflow = yadage_engine.get_workflow(workflow_id)
    if workflow:
        return jsonify(api.workflow_to_json(workflow))
    else:
        abort(404)


@app.route('/workflows/<string:workflow_id>', methods=['DELETE'])
def delete_workflow(workflow_id):
    """DELETE - Workflow

    Deletes the workflow with the given id.
    """
    # Delete workflow with given identifier. The result is True if the workflow
    # was successfully deleted and False if the workflow identifier is unknown.
    # Abort with NOT FOUND (404) in the latter case. Send an empty response
    # in case of success.
    if yadage_engine.delete_workflow(workflow_id):
        return '', 204
    else:
        abort(404)


@app.route('/workflows/<string:workflow_id>/apply', methods=['POST'])
def apply_rules(workflow_id):
    """POST - Workflow extension

    Extend workflow with set of rules. Expects a list of rule identifiers.
    """
    # Abort with BAD REQUEST if the request body is not in Json format or
    # does not contain a reference to a list of appliable rules
    if not request.json:
        abort(400)
    json_obj = request.json
    if not 'rules' in json_obj:
        abort(400)
    # Apply rule instances to given workflow. The result is False if the
    # workflow does not exist.
    if not yadage_engine.apply_rules(workflow_id, json_obj['rules']):
        abort(404)
    # Return the descriptor of the modified workflow.
    workflow = yadage_engine.get_workflow(workflow_id)
    return jsonify(api.workflow_descriptor_to_json(workflow))


@app.route('/workflows/<string:workflow_id>/files')
def get_workflow_files(workflow_id):
    """GET - Workflow directory listing

    Recursive listing of all files in the workflow working directory.
    """
    # Abort with 404 if the workflow does not exist.
    workflow = yadage_engine.get_workflow(workflow_id)
    if not workflow:
        abort(404)
    # Return the workflow directory listing
    directory_name = os.path.join(WORK_BASE, workflow_id)
    return jsonify(api.workflow_files(directory_name, workflow_id))


@app.route('/workflows/<string:workflow_id>/submit', methods=['POST'])
def submit_tasks(workflow_id):
    """POST - Submit task

    Submit a set of tasks for execution. Expects a list of node identifiers.
    """
    # Abort with BAD REQUEST if the request body is not in Json format or
    # does not contain a reference to a list of runnable nodes
    if not request.json:
        abort(400)
    json_obj = request.json
    if not 'nodes' in json_obj:
        abort(400)
    # Submit nodes for execution. The result is False if the workflow does
    # not exist.
    success = yadage_engine.submit_nodes(workflow_id, json_obj['nodes'])
    if not success:
        abort(404)
    # Return the descriptor of the modified workflow.
    workflow = yadage_engine.get_workflow(workflow_id)
    return jsonify(api.workflow_descriptor_to_json(workflow))


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(error):
    """404 JSON response generator."""
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.errorhandler(500)
def internal_error(exception):
    """Exception handler that logs exceptions."""
    app.logger.error(exception)
    return make_response(jsonify({'error': str(exception)}), 500)


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    # Relevant documents:
    # http://werkzeug.pocoo.org/docs/middlewares/
    # http://flask.pocoo.org/docs/patterns/appdispatch/
    from werkzeug.serving import run_simple
    from werkzeug.wsgi import DispatcherMiddleware
    # Switch logging on if not in debug mode
    if app.debug is not True:
        import logging
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(os.path.join(LOG_DIR, 'yadage-engine.log'), maxBytes=1024 * 1024 * 100, backupCount=20)
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        app.logger.addHandler(file_handler)
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple('0.0.0.0', PORT, application, use_reloader=app.config['DEBUG'])
