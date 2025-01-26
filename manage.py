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

@cli.command("collector")
def collector():
    """
    Run the data collector service.
    """
    from services.collector.main import main as collector_main
    click.echo("Starting collector service...")
    asyncio.run(collector_main())

@cli.command("aggregator")
def aggregator():
    """
    Run the data aggregator service.
    """
    from services.aggregator.main import main as aggregator_main
    click.echo("Starting aggregator service...")
    asyncio.run(aggregator_main())

@cli.command("migrate")
def migrate():
    """
    Runs database migrations.
    """
    click.echo("Running migrations...")
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

if __name__ == "__main__":
    cli()
