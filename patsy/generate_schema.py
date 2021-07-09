import io

from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph

from .database import Session


def generate_schema_command(dot=None, png=None):
    """
    Called by the CLI to perform the "generate_schema" command.
    :param dot: The filepath of the .dot file to generate.
    :param png: The filepath of the .png file to generate.
    """
    session = Session()

    # create the pydot graph object by autoloading all tables via a bound
    # metadata object
    graph = create_schema_graph(
        metadata=MetaData(bind=session.bind),
        rankdir='LR',
        concentrate=False
        )

    files = []

    if dot:
        graph.write_dot(dot)
        files.append(dot)

    if png:
        graph.write_png(png)
        files.append(png)

    return files
