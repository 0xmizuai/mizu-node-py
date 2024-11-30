# Use an official Python 3.11 runtime as a parent image
FROM docker.1ms.run/python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y \
    curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get clean

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy only the necessary files for Poetry to install dependencies
COPY poetry.lock pyproject.toml /app/

# Install dependencies
RUN poetry config virtualenvs.create false && poetry install --no-root --no-interaction --no-ansi

# Copy the entire project into the container
COPY . /app

# Expose port for FastAPI
EXPOSE 8000

RUN ls -la

# Default command for development mode
CMD ["poetry", "run", "start"]
