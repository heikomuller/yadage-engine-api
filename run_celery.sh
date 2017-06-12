#!/bin/sh

#su -m celery_user -c "celery worker -A packtivity.asyncbackends:default_celeryapp -I packtivity.asyncbackends -l debug"
celery worker -A packtivity.asyncbackends:default_celeryapp -I packtivity.asyncbackends -l debug
