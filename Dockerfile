FROM python:2.7

COPY ./requirements.txt /app/requirements.txt
COPY ./setup.* /app/
COPY ./yadageengine/*.py /app/yadageengine/

WORKDIR /app

RUN pip install --no-cache-dir -e .

CMD ["python", "yadageengine"]
