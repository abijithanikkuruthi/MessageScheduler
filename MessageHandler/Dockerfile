FROM python:3

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
ENV TZ=Europe/Amsterdam
CMD [ "python3", "main.py" ]