from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

# Create async engines
postgres_engine = create_async_engine(
    os.getenv('POSTGRESQL_DATABASE', 'postgresql+asyncpg://user:password@postgres:5432/default'),
    echo=True,  # Set to False in production
)

timescale_engine = create_async_engine(
    os.getenv('TIMESCALE_DATABASE', 'postgresql+asyncpg://user:password@timescale:5432/default'),
    echo=True,  # Set to False in production
)

# Create async session factories
AsyncPostgresSession = sessionmaker(
    postgres_engine, expire_on_commit=False, class_=AsyncSession
)

AsyncTimescaleSession = sessionmaker(
    timescale_engine, expire_on_commit=False, class_=AsyncSession
)

# Base class for declarative models
Base = declarative_base()

# Dependency to get DB sessions
async def get_postgres_session() -> AsyncSession:
    async with AsyncPostgresSession() as session:
        yield session

async def get_timescale_session() -> AsyncSession:
    async with AsyncTimescaleSession() as session:
        yield session

# Initialize and close functions (optional, depending on your needs)
async def init_db():
    async with postgres_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def close_db():
    await postgres_engine.dispose()
    await timescale_engine.dispose()