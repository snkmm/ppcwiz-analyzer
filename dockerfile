FROM python:3.8

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip3 install -r requirements.txt
RUN pip3 install spacy==2.3.5
RUN python3 -m spacy download en_core_web_sm

COPY . .

CMD [ "python3", "main.py" ]