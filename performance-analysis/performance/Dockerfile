FROM python:3

ARG UNAME=appuser
ARG UID=1000
ARG GID=1000
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME
USER $UNAME

COPY . /tmp/
ENV TZ=Europe/Amsterdam
ENV PYTHONUNBUFFERED=TRUE
RUN cd /tmp && sh /tmp/setup.sh
WORKDIR /app/performance-analysis/performance
CMD [ "sh", "run.sh" ]
