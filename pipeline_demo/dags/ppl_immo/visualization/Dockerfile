FROM python:3.10

RUN mkdir /app
COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

#RUN adduser --disabled-password --gecos '' airflow

#USER airflow

CMD ["python", "visualization.py"]