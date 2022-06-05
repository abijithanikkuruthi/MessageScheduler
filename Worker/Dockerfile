FROM rust:1.61.0

COPY . /app
WORKDIR /app
ENV TZ=Europe/Amsterdam
RUN sh setup.sh
CMD [ "sh", "run.sh" ]