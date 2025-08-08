FROM python:3.11-slim

# Evita bytecode y buffers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Dependencias del sistema (conector, build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

# Instala Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el proyecto
COPY . .

# Comando por defecto: pipeline end-to-end
CMD ["bash", "run_pipeline.sh"]

# docker build -t oil-gas-dashboard .
