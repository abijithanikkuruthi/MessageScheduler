FROM docker:latest

RUN apk add --no-cache tzdata
ENV TZ=Europe/Amsterdam
COPY run.sh /run.sh
CMD [ "sh", "/run.sh" ]