# Yadage Web API and User-Interface

This repository contains the Web API for Yadage. The Web API is a Flask application that orchestrates the execution of workflows and serves data for a Web UI. The UI is not part of this package.


## Setup

The Web API uses Flask and is intended to run in a Python virtual environment. Set up the environment using the following commands:

```
cd src/main/py
virtualenv venv
source venv/bin/activate
pip install adage
pip install cap-schemas
pip install packtivity
pip install yadage
pip install flask
pip install -U flask-cors
pip install pymongo
pip install -U celery[redis]==3.1.24
deactivate
```


## Run

### Prerequisites

Before running the Web API app a few components need to be in place and running. Note that currently there is no option to configure any of these components without modifying the code.

#### Yadage Backend
Different Yadage backends have different requirements. The current implementation uses the Yadage Celery backend. For this backend Redis Server and Celery need to be running:

```
redis-server
celery worker -A yadage.backends.celeryapp -I yadage.backends.packtivity_celery -l debug
```

#### MongoDB
The Web API currently uses MongoDB as a persitent storage backend for workflow states. The implementation expects MongoDB to be running at localhost without any authentication. It uses a database called yadage.


#### Workflow Template Repository Server

The Web UI requires access to a workflow template repository server. The server can be run locally (follow instructions in [GitHub repository](https://github.com/heikomuller/yadage-workflow-repository))or the default server at http://cds-swg1.cims.nyu.edu/workflow-repository/api/v1/ can be used.


### Web API Server

After the virtual environment is set up, the Web API can be run using the following command:

```
./server.py
	[-a | --path] <app-path>
	[-d | --debug]
	[-f | --files] <base-directory>
	[-l | --logs] <log-directory>
	[-p | --port] <port-number>
	[-s | --server] <server-url>

app-path:
	Path on the server under which the app is accessible (Default: /yadage/api/v1/yadage/api/v1)
-d/--debug:
	Switch debug mode on (Default: False)
base-directory:
	Path to shared file system where workflow inputs and outputs are stored (Default: ../../backend/data/)
log-directory:
	Path to directory where log files are stored, if not running in debug mode. (Default: ../../backend/data/)
port-number:
	Port at which the app is running (Default: 5006)
server-url:
	URL of the server running the app (Default: http://localhost)
```

When running the Web API with the default command line parameters the application will be accessible on the local host at URL http://localhost:5006/yadage/api/v1/.
