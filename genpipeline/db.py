"""
Database pipelines
==================

The CSV handling code using COPY only works with PostgreSQL and requires Psycopg2 >2.5.

API
---

.. autofunction:: run_query
.. autofunction:: run_sqlalchemy
.. autofunction:: inserter
.. autofunction:: upload_csv
"""

import csv
import io
import itertools
import logging
from . import pipesource, pipefilter, iter_sink
from contextlib import closing
from sqlalchemy import sql

_log = logging.getLogger(__name__)

class tabdialect:
    delimiter = '\t'
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL


def execute(connection, query, parameters=None):
    """Execute query on a DBAPI connection and return results

    :param connection: DBAPI connection object
    :param query: query string
    :param parameters: optional parameters, passed to cursor.execute method
    """

    cursor = connection.cursor()
    cursor.execute(query, parameters)
    return cursor


@pipesource
def run_query(conn, query, params=None, target=None):
    """Pipeline source pushing rows from a SQL query

    :param conn: DBAPI connection object
    :param query: query text
    :param params: parameters for query, passed through to the cursor.execute method
    """

    try:
        for row in execute(conn, query, params):
            target.send(row)
        target.close()
    except Exception as e:
        try:
            target.throw(e)
        except StopIteration:
            pass
        raise e


@pipesource
def run_sqlalchemy(engine, query, target=None):
    """Pipeline source pushing rows (as dicts) from a SQLAlchemy query

    :param engine: SQLAlchemy engine (:py:class:`sqlalchemy.engine.Engine`)object
    :param query: SQLAlchemy query (:py:class:`sqlalchemy.sql.expression.Select` or similar)
    """

    try:
        if engine.dialect.name == "sqlite":
            # sqlite doesn't handle updates while querying another table
            for row in list(engine.connect().execute(query)):
                target.send(dict(row))
        else:
            for row in engine.connect().execute(query):
                target.send(dict(row))
        target.close()
    except Exception as e:
        try:
            target.throw(e)
        except StopIteration:
            pass
        raise e


@pipefilter
def inserter(conn, table, columns):
    """Sink: insert rows into a database table"""

    sql = "INSERT INTO {table} ({columns}) VALUES ({placeholders})".format(
        table=table,
        columns=", ".join(column for column in columns),
        placeholders=", ".join("%s" for _ in columns))
    cursor = conn.cursor()
    while True:
        row = (yield)
        cursor.execute(sql, row)


class CSVFileAdapter:
    """File-like object that generates CSV suitable for copying into PostgreSQL"""

    def __init__(self, data, line_count=100, dialect=csv.excel, null_string=""):
        """
        :param data: data to be exported to CSV - most likely an iterator
        :param line_count: the number of lines to be read before exporting a chunk of CSV
        """

        self._data = data
        self._line_count = line_count
        self._dialect = dialect
        self._null_string = null_string

    def read(self, size=8192):
        """The size argument is currently ignored"""

        try:
            csv_file = io.StringIO()
            csv_data = csv.writer(csv_file, dialect=self._dialect)
            lines = itertools.islice(self._data, self._line_count)
            if self._null_string:
                lines = ([self._null_string if x is None else x for x in line] for line in lines)
            csv_data.writerows(lines)
            return csv_file.getvalue()
        except Exception:
            # Log exceptions here as they'll be caught by psycopg2 and raised as something else
            _log.error("Error in CSVFileAdapter.read", exc_info=True)
            raise

    def readline(self):
        try:
            csv_file = io.StringIO()
            csv_data = csv.writer(csv_file, dialect=self._dialect)
            lines = itertools.islice(self._data, 1)
            if self._null_string:
                lines = ([self._null_string if x is None else x for x in line] for line in lines)
            csv_data.writerows(lines)
            return csv_file.getvalue()
        except Exception:
            # Log exceptions here as they'll be caught by psycopg2 and raised as something else
            _log.error("Error in CSVFileAdapter.readline", exc_info=True)
            raise


@iter_sink
def upload_csv(data, engine, table, columns):
    """Insert data to a database table using the PostgreSQL COPY command, with CSV format

    :param data: iterable yielding dicts to insert, indexed with columns to generate row
    :param conn: psycopg2 database connection
    :param table: name of table (including schema if appropriate) to insert into
    :param columns: list of columns to insert into 
    """
    with closing(engine.raw_connection()) as rawconn:
        with rawconn.connection as conn:
            with conn.cursor() as cursor:
                data = ([row.get(column) for column in columns] for row in data)
                if hasattr(cursor, "copy_from"):
                    cursor.copy_from(CSVFileAdapter(data, dialect=tabdialect, null_string=r"\N"),
                                 table,
                                 columns=columns)
                else:
                    stmt = "INSERT INTO %s (%s) VALUES (%s)" % (table, ",".join(columns), 
                                                                ",".join(["?"] * len(columns)))
                    batch = []
                    for row in data:
                        batch.append(row)
                        if len(batch) >= 100:
                            cursor.executemany(stmt, batch)
                            batch = []
                    if batch:
                        cursor.executemany(stmt, batch)
