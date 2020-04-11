FROM tiangolo/uvicorn-gunicorn-fastapi:latest

RUN pip install pydantic[email]
RUN pip install requests
RUN pip install pytest
RUN pip install sqlalchemy
RUN pip install psycopg2
RUN pip install -U fastapi

COPY ./app app
