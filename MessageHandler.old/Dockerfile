FROM python:3

COPY . /app
WORKDIR /app
ENV TZ=Europe/Amsterdam
ENV PYTHONUNBUFFERED=TRUE
RUN sh setup.sh
CMD [ "sh", "run.sh" ]