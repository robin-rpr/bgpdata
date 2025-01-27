#!/usr/bin/env python
import click
import sys
import os
import asyncio
import importlib
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

@cli.command("proxy")
@click.argument('type', required=True)
def proxy(_type):
    """
    Proxy data from a source.
    """
    try:
        module_path = f"proxies.{_type}.main"
        module = importlib.import_module(module_path)
        asyncio.run(module.main())
    except ModuleNotFoundError:
        click.echo(f"Error: Proxy for type '{_type}' not found.", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

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
