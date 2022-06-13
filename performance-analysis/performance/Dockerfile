FROM python:3

COPY . /tmp/
ENV TZ=Europe/Amsterdam
ENV PYTHONUNBUFFERED=TRUE
RUN cd /tmp && sh /tmp/setup.sh
WORKDIR /app/performance-analysis/performance
CMD [ "sh", "run.sh" ]