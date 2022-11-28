FROM python:3.10.8

COPY requirements.txt .

RUN pip install -r requirements.txt 

WORKDIR /app

COPY ["etl.py", "awesome-highway-358007-5eb23d1d599f.json", "./"] 

ENTRYPOINT [ "python3", "etl.py" ]

# CMD python3 etl.py

