FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies via uv (uses pyproject.toml and uv.lock)
COPY pyproject.toml uv.lock ./
RUN uv sync

# Copy code into venv (no pip install, flat layout relies on CWD imports)
COPY . .
# RUN pip install --no-cache-dir .  # removed—flat layout breaks pip install

# Add uv-created venv to PATH so commands target the .venv
ENV PATH="/app/.venv/bin:$PATH"
# Ensure Python sees the agents package at runtime
ENV PYTHONPATH="/app"

# Default command: run startup script that manages all agents
CMD ["uv", "run", "python", "start_agents.py"]
