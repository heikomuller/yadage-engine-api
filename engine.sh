#!/bin/sh
celery worker -A packtivity.asyncbackends:default_celeryapp -I packtivity.asyncbackends -l debug&
python -u yadageengine
