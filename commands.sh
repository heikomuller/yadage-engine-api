redis-server
sudo -u www-data bash -c "venv/bin/celery worker -A yadage.backends.celeryapp -I yadage.backends.packtivity_celery -l debug"
