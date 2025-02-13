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
    Runs the collector.
    """

    import collector.main as module
    asyncio.run(module.main())

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
