# Use the latest Python image
FROM python:3.8-bookworm

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/*

# Install pip dependencies for simdjson
RUN pip install --no-cache-dir simdjson gitpython tabulate

# Set up a user for VS Code to use
RUN useradd -ms /bin/bash vscode
USER vscode

# Set the working directory
WORKDIR /workspace