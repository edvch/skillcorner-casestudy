FROM python:3.12

WORKDIR /casetudy

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python3", "casetudy/interface/main.py"]