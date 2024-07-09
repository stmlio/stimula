"""
This script provides the context for the database connection.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import threading

from sqlalchemy import create_engine, MetaData

cnx_context = threading.local()


def get_metadata(cnx):
    engine = create_engine('postgresql://', creator=lambda: cnx)

    # Create a MetaData instance
    metadata = MetaData()

    # Reflect the existing database schema
    metadata.reflect(bind=engine)

    return metadata
