FROM python:3.9
ENV PYTHONUNBUFFERED=1

WORKDIR /convert_2_files
COPY . convert_2_files

RUN pip install pipenv
RUN pipenv install --skip-lock

CMD ["python", "./main.py"]



