FROM python:3.7.2

RUN useradd -d /home/python -mrs /bin/bash python

WORKDIR /home/python/app

USER python

COPY . .

RUN pip install --no-cache-dir -r requirements.txt --user 

CMD ["/usr/local/bin/python3", "-u", "./main.py"]