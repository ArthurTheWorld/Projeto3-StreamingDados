FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y bash

RUN pip install --no-cache-dir \
    delta-spark==3.3.0 \
    yfinance==0.2.33 \
    multitasking==0.0.11 \
    requests \
    pandas \
    numpy \
    google-cloud-storage==2.14.0 \
    google-auth==2.28.1 \
    google-api-python-client==2.117.0

WORKDIR /app

CMD ["tail", "-f", "/dev/null"]
