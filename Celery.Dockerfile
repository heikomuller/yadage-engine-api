#FROM python:2.7

FROM fedora
RUN dnf install -y gcc gcc-c++ graphviz-devel ImageMagick python-devel libffi-devel openssl openssl-devel unzip nano autoconf automake libtool redhat-rpm-config; dnf clean all

COPY ./requirements.txt /app/requirements.txt
COPY ./setup.* /app/
COPY ./yadageengine/*.py /app/yadageengine/
COPY ./run_celery.sh /app/

WORKDIR /app

RUN curl https://bootstrap.pypa.io/get-pip.py | python -
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.9.1  -o /usr/bin/docker && chmod +x /usr/bin/docker
RUN pip install -e . --process-dependency-links

#RUN pip install --no-cache-dir -e .
#RUN chmod +x /app/run_celery.sh
#RUN adduser --disabled-password --gecos '' celery_user

CMD ["/app/run_celery.sh"]
