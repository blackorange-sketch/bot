FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

# Бот сам перепідключається при розриві WS-з'єднання,
# але на випадок падіння всього процесу хост-платформа має його рестартувати
# (Fly.io / Railway / systemd роблять це автоматично)
CMD ["python", "-u", "bot.py"]
