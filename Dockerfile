FROM python:3.6-buster

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY td_sql_runner.py /app/

CMD python /app/td_sql_runner.py --repo /src/scripts --dbparam /src/db.txt