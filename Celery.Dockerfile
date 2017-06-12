FROM python:2.7

COPY ./requirements.txt /app/requirements.txt
COPY ./setup.* /app/
COPY ./yadageengine/*.py /app/yadageengine/
COPY ./run_celery.sh /app/

WORKDIR /app

RUN pip install --no-cache-dir -e .
RUN chmod +x /app/run_celery.sh
RUN adduser --disabled-password --gecos '' celery_user

CMD ["/app/run_celery.sh"]
