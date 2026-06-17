# Utiliser l'image de base Spark officielle (qui contient déjà Spark et Java)
FROM apache/spark:3.5.1

# Passer en root pour installer les paquets système
USER root

# Installer pip et les utilitaires nécessaires
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail
WORKDIR /app

# Copier les dépendances
COPY requirements.txt .

# Installer les dépendances Python
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copier le reste du projet
COPY . .

# Exposer le port de Streamlit
EXPOSE 8501

# Lancer l'application par défaut (sera surchargé par docker-compose pour les workers)
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
