FROM python:3.9-slim
WORKDIR /app
RUN pip install flask pika
COPY . .
CMD ["python", "-u", "app.py"]