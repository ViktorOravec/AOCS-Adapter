FROM python:3.9-slim

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app
EXPOSE 7001

CMD ["uvicorn", "app.main:app", "--log-config", "/code/app/log_conf.yaml", "--port", "7001", "--host", "0.0.0.0", "--log-level",  "critical"]