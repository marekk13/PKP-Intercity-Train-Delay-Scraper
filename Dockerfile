FROM selenium/standalone-chrome:latest

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/outputs

CMD ["python", "get_train_data.py"]
