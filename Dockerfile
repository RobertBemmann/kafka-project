FROM python:3.10-alpine
ADD . /code
WORKDIR /code
RUN apk update && \
    pip install -r requirements.txt
CMD ["python3.10","main.py"]