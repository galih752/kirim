FROM python:3.11-bookworm

COPY requirements.txt /

WORKDIR /

RUN pip install -r requirements.txt \
    && playwright install --with-deps

WORKDIR /app

COPY main2.py /app

ENTRYPOINT [ "python", "main2.py" ]
