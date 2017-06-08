# Yadage Web API and User-Interface

A basic Web API for [Adage](https://github.com/diana-hep/adage.git) workflow templates. Provides access to a repository of templates for the .

This repository contains a Web API for the [Yadage Web Workflow Engine](https://github.com/heikomuller/yadage-engine-api). The Web API is a Flask application that orchestrates the execution of workflows and serves data for the [Yadage UI](https://github.com/diana-hep/yadage-webui).


The **documentation** of the API is located in
```
doc/html
```


## Setup

Below is a simple example to setup the workflow engine server after cloning the GihHub repository. The example uses a Python virtual environment:

```
virtualenv venv
source venv/bin/activate
pip install -e .
```


## Run

### Prerequisites

Before running the Web API app a few components need to be in place and running.

#### Yadage Backend
Different Yadage backends have different requirements. The current implementation uses the Yadage Celery backend. For this backend Redis Server and Celery need to be running:

```
redis-server
celery worker -A packtivity.asyncbackends.default_celeryapp -l debug
```

#### MongoDB
The Web API currently uses MongoDB as a persistent storage backend for workflow states. The connection Uri and database name can be specified in the configuration file. Authentication is currently not supported.


### Web API Server

The workflow repository API is implemented as a Flask application. After the virtual environment is set up, the Web API can be run using the following command:

```
python yadageengine
```

## Configuration

The API is configured using a configuration file. Configuration files are in YAML format. The default configuration file is `config/config.yaml`. At startup, the workflow engine server first tries to load the configuration file that is specified in the environment variable **YADAGE_ENGINE_CONFIG**. If the variable is not set or the file does not exists the server tries to access file `config.yaml` in the working directory. If no configuration file is found the values from the default file in the GitHub repository are used.

Below is the content of the default configuration file:

```
properties:
    - key: 'server.apppath'
      value : '/yadage-engine/api/v1'
    - key: 'server.url'
      value : 'http://localhost'
    - key: 'server.port'
      value: 25011
    - key: 'app.name'
      value: 'Yadage Workflow Engine API'
    - key: 'app.doc'
      value: 'http://cds-swg1.cims.nyu.edu/yadage-engine/api/v1/doc/index.html'
    - key: 'app.debug'
      value: true
    - key: 'app.logdir'
      value: './log'
    - key: 'db.workdir'
      value: './data'
    - key:  'mongo.db'
      value: 'yadage'
```

Entries in the configuration file are (key,value)-pairs. The following are valid keys:

- **server.url**: Url of the web server where the API is running. Used as prefix to generate Url's for API resources.
- **server.port**: Port on the server where Flask runs on.
- **server.apppath**: Path of the Flask application that runs the API. The combination of *server.url*, *server.port*, and *server.app* is expected the root Url for the API.
- **app.name**: Descriptive name for a running API instance
- **app.doc**: Url to the Html file containing the API documentation.
- **app.debug**: Switch debugging ON/OFF.
- **app.logdir**: Path to directory for log files (optional).
- **db.workdir**: Path to local directory under which workflow files are being stored
- **mongo.db**: Name of the MongoDB database where workflow information is stored
- **mongo.uri** (optional): MongoDB connection Uri used by the MongoDB client


## Docker

The workflow template API is available as a docker image on Docker Hub:

```
docker pull heikomueller/yadage-engine
```

The command to run the docker image with the default configuration is:

```
docker run -d -p 25011:25011 yadage-engine
```

If you want to use a custom configuration you can use the follwoing command:

```
docker run -d -p 5000:5000 -e YADAGE_ENGINE_CONFIG="/config/config.yaml" -v /home/user/yadage-engine/config:/config heikomueller/yadage-engine
```
The command assumes a local config file `/home/user/yadage-engine/config/config.yaml` is used and that the **server.port** is set to 5000.
