#!venv/bin/python

"""Yadage Workflow Engine - Web Server

The Yadage Web API is the access point to the Yadage Engine API. The Web server
represents a thin layer around the Yadage Engine API that wraps all relevant API
methods for users to interact with the workflow engine via Http requests.
"""

from flask import Flask, abort, jsonify, make_response, request, send_from_directory
from flask_cors import CORS
import os
from pymongo import MongoClient
import urllib2
import yaml

from engine import YADAGEEngine


# ------------------------------------------------------------------------------
#
# Gobal Constants
#
# ------------------------------------------------------------------------------

# Environment Variable containing path to config file.
ENV_CONFIG = 'YADAGE_ENGINE_CONFIG'
# Default configuration file on GitHub
WEB_CONFIG_FILE_URI = 'https://raw.githubusercontent.com/heikomuller/yadage-engine-api/master/config/config.yaml'


# ------------------------------------------------------------------------------
#
# App Configuration and Initialization
#
# ------------------------------------------------------------------------------

# Read configuration information from file. Attempts to read the file that is
# specified in the value of the environment variable ENV_CONFIG. If the
# variable is not set a file 'config.yaml' in the current working directory
# will be used. The default configuration is read first from the GitHub
# repository. Default values are overwritten by local configurations (if any).
#def_conf = yaml.load(urllib2.urlopen(WEB_CONFIG_FILE_URI).read())['properties']
#config = {kvp['key'] : kvp['value'] for kvp in def_conf}
config = {}
LOCAL_CONFIG_FILE = os.getenv(ENV_CONFIG)
obj = None
if os.path.isfile(LOCAL_CONFIG_FILE):
    with open(LOCAL_CONFIG_FILE, 'r') as f:
        obj = yaml.load(f.read())
elif os.path.isfile('./config.yaml'):
    with open('./config.yaml', 'r') as f:
        obj = yaml.load(f.read())
if not obj is None:
    for kvp in obj['properties']:
        config[kvp['key']] = kvp['value']

# The following is a list of valid/expected properties (*optional):
#
# server.apppath : Application path part of the Url to access the app
# server.url : Base Url of the server where the app is running
# server.port: Port the server is running on
# app.doc : Url to web service documentation
# app.debug: Switch debugging ON/OFF
# app.logdir : Directory for log files
# db.workdir : Path to local directory for workflow files
# mongo.db : Name of MongoDB database containing workflow state information
# mongo.uri (optional): Uri containing MongoDB host and port (e.g.,
#             'mongodb://user:pwd@example.com?authMechanism=SCRAM-SHA-1')

# Switch debugging ON/OFF
DEBUG = config['app.debug']

# Ensure that the workflow base directory exists
WORK_BASE = os.path.abspath(config['db.workdir'])
if not os.access(WORK_BASE, os.F_OK):
    os.makedirs(WORK_BASE)

# Directory for log files. Logging is only used if not in DEBUG mode. Create
# the directory if it does not exists
if 'app.logdir' in config:
    LOG_DIR = os.path.abspath(config['app.logdir'])
else:
    LOG_DIR = None
if not DEBUG:
    if not os.access(LOG_DIR, os.F_OK):
        os.makedirs(LOG_DIR)


# ------------------------------------------------------------------------------
# Initialize the YADAGE Server engine API.
# ------------------------------------------------------------------------------

api = YADAGEEngine(config)


# ------------------------------------------------------------------------------
# Initialize the Web app
# ------------------------------------------------------------------------------

# Create the app and enable cross-origin resource sharing
app = Flask(__name__, static_url_path=WORK_BASE)
app.config['APPLICATION_ROOT'] = config['server.apppath']
app.config['PORT'] = config['server.port']
app.config['DEBUG'] = DEBUG
if not LOG_DIR is None:
    app.config['LOG_DIR'] = LOG_DIR
CORS(app)


# ------------------------------------------------------------------------------
#
# API
#
# ------------------------------------------------------------------------------

@app.route('/')
def get_welcome():
    """GET - Welcome Message

    Main object for the web service. Contains the service name and a list of
    references for clients to interact with the API.
    """
    return jsonify(api.get_description())


@app.route('/files/<path:path>')
def send_file(path):
    """GET - Workflow output file

    Returns a file from local dist that was created by a completed workflow
    step.
    """
    return send_from_directory(WORK_BASE, path)


@app.route('/workflows')
def list_workflows():
    """GET - Workflow Listing

    Returns a list of workflows currently managed by the workflow engine.
    """
    # Get list of worklows and return a list of workflow descriptors
    return jsonify(api.list_workflows())


@app.route('/workflows', methods=['POST'])
def create_workflow():
    """POST - Submit RECAST request

    Handles request to run a workflow template.
    """
    # Abort with BAD REQUEST if the request body is not in Json format orelse
    # does not contain a reference to a workflow template
    if not request.json:
        abort(400)
    # Get the Url for the workflow template
    if not 'template' in request.json:
        abort(400)
    template_url = request.json['template']
    # Get dictionary of user provided input data (if given)
    parameters = {}
    if 'parameters' in request.json:
        for para in request.json['parameters']:
            if not 'key' in para or not 'value' in para:
                abort(400)
            parameters[para['key']] = para['value']
    # Get the user provided workflow name.
    name = None
    if 'name' in request.json:
        name = request.json['name'].strip()
    # Submit request to workflow engine
    try:
        workflow = api.create_workflow(
            template_url,
            parameters=parameters,
            name=name
        )
        # Send response with code 201 (HTTP Created)
        return jsonify(workflow), 201
    except ValueError as ex:
        abort(400)

@app.route('/workflows/<string:workflow_id>')
def get_workflow(workflow_id):
    """GET - Workflow

    Retrieve workflow with given identifier.
    """
    # Get workflow from engine. Abort with NOT FOUND (404) if result is null.
    workflow = api.get_workflow(workflow_id)
    if workflow is None:
        abort(404)
    return jsonify(workflow)


@app.route('/workflows/<string:workflow_id>', methods=['DELETE'])
def delete_workflow(workflow_id):
    """DELETE - Workflow

    Deletes the workflow with the given id.
    """
    # Delete workflow with given identifier. The result is True if the workflow
    # was successfully deleted and False if the workflow identifier is unknown.
    # Abort with NOT FOUND (404) in the latter case. Send an empty response
    # in case of success.
    if api.delete_workflow(workflow_id):
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
    # Apply rule instances to given workflow. The result is None if the
    # workflow does not exist. THe method throws a ValueError if any of the
    # selected rules is not applicable.
    try:
        workflow = api.apply_rules(workflow_id, json_obj['rules'])
        if workflow is None:
            abort(404)
        # Return the descriptor of the modified workflow.
        return jsonify(workflow)
    except ValueError as ex:
        print 'ERROR'
        print ex
        abort(400)


@app.route('/workflows/<string:workflow_id>/files')
def get_workflow_files(workflow_id):
    """GET - Workflow directory listing

    Recursive listing of all files in the workflow working directory.
    """
    # Get a list of workflow files. The result is None if the workflow does
    # not exists.
    files = api.get_workflow_files(workflow_id)
    if files is None:
        abort(404)
    return jsonify(files)


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
    # Submit nodes for execution. The result is None if the workflow does
    # not exist. Raises ValueError if any of the given nodes is not submittable.
    try:
        workflow = api.submit_nodes(workflow_id, json_obj['nodes'])
        if workflow is None:
            abort(404)
    except ValueError as ex:
        abort(400)
    # Return the descriptor of the modified workflow.
    return jsonify(workflow)


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
        file_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, 'yadage-engine.log'),
            maxBytes=1024 * 1024 * 100,
            backupCount=20
        )
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        app.logger.addHandler(file_handler)
    # Load a dummy app at the root URL to give 404 errors.
    # Serve app at APPLICATION_ROOT for localhost development.
    application = DispatcherMiddleware(Flask('dummy_app'), {
        app.config['APPLICATION_ROOT']: app,
    })
    run_simple('0.0.0.0', app.config['PORT'], application, use_reloader=app.config['DEBUG'])
