from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

PostgreSQL = sessionmaker(
    create_async_engine(
        os.getenv('POSTGRESQL_DATABASE'), echo=False
    ), expire_on_commit=False, class_=AsyncSession
)