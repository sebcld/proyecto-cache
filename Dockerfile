FROM python:3.11-slim

WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Crear directorio para métricas
RUN mkdir -p metrics/results metrics/experiments

# Punto de entrada por defecto
CMD ["python", "main.py"]