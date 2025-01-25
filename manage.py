#!/usr/bin/env python
import click
from flask.cli import FlaskGroup
from alembic.config import Config
from alembic import command

# Import your Flask app or the factory function
# If you use an app factory, import it:
from app import main as app_main

# Import your services
from services.collector.main import main as collector_main
from services.aggregator.main import main as aggregator_main

@click.group(cls=FlaskGroup)
def cli():
    """
    This is our main entry point for commands.
    """
    pass

@cli.command("runserver")
def runserver():
    """
    Starts the Flask development server.
    """
    app = app_main()
    app.run(debug=True)

@cli.command("collector")
def collector():
    """
    Run the data collector service.
    """
    click.echo("Starting collector service...")
    collector_main()

@cli.command("aggregator")
def aggregator():
    """
    Run the data aggregator service.
    """
    click.echo("Starting aggregator service...")
    aggregator_main()

@cli.command("migrate")
def migrate():
    """
    Runs alembic migrations (equivalent to `alembic upgrade head`).
    """
    click.echo("Running migrations with Alembic...")
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

if __name__ == "__main__":
    cli()
