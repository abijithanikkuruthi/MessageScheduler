FROM python:3

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
ENV TZ=Europe/Amsterdam
ENV PYTHONUNBUFFERED=TRUE
CMD [ "gunicorn", "main:app", "--bind", "0.0.0.0:8000", "--workers", "1" ]