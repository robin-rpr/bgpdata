#!/usr/bin/env python
import click
import sys
import os
import asyncio
from flask.cli import FlaskGroup
from alembic.config import Config
from alembic import command

# Ensure the root directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def create_my_app():
    """FlaskGroup needs this function to create the app."""
    from app import create_app
    return create_app()

@click.group(cls=FlaskGroup, create_app=create_my_app)
def cli():
    """BGPDATA Management Script."""
    pass

@cli.command("run")
@click.option("--workers", default="1", help="Number of workers to run with Gunicorn")
@click.option("--host", default="localhost", help="Host to run the application on")
@click.option("--port", default=8080, help="Port to run the application on")
def run(workers, host, port):
    """
    Runs the application.
    """
    import subprocess
    subprocess.run([
        "gunicorn", 
        "--bind", f"{host}:{port}", 
        "--workers", workers,
        "--worker-class", "uvicorn.workers.UvicornWorker",
        "app:asgi_app"
    ])
    

@cli.command("collector")
def collector():
    """
    Runs the collector.
    """

    import collector.main as module
    asyncio.run(module.main())

@cli.command("migrate")
def migrate():
    """
    Runs database migrations.
    """
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

if __name__ == "__main__":
    cli()
