
FROM python:3.12


WORKDIR /app


COPY load_data.py requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80


CMD ["python", "load_data.py"]
