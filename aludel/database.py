from alchimia import TWISTED_STRATEGY

from sqlalchemy import MetaData, Table, Column, create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateTable


def get_engine(conn_str, reactor):
    return create_engine(conn_str, reactor=reactor, strategy=TWISTED_STRATEGY)


class TableMissingError(Exception):
    pass


class make_table(object):
    def __init__(self, *args, **kw):
        self.args = args
        self.kw = kw

    def make_table(self, name, metadata):
        return Table(name, metadata, *self.copy_args(), **self.kw)

    def copy_args(self):
        for arg in self.args:
            if isinstance(arg, Column):
                yield arg.copy()
            else:
                yield arg


class PrefixedTableCollection(object):
    """
    Collection of database tables sharing a common prefix.

    Each table is prefixed with the collection type and name.

    The collection type defaults to the class name, but the
    :attr:`COLLECTION_TYPE` class attribute may be set to override this.
    """

    COLLECTION_TYPE = None

    def __init__(self, name, connection):
        self.name = name
        self._conn = connection
        self._metadata = MetaData()
        for attr in dir(self):
            attrval = getattr(self, attr)
            if isinstance(attrval, make_table):
                setattr(self, attr, attrval.make_table(
                    self.get_table_name(attr), self._metadata))

    @classmethod
    def collection_type(cls):
        ctype = cls.COLLECTION_TYPE
        if ctype is None:
            ctype = cls.__name__
        return ctype

    def get_table_name(self, name):
        return '%s_%s_%s' % (self.collection_type(), self.name, name)

    def _create_table(self, trx, table):
        # This works around alchimia's current inability to create tables only
        # if they don't already exist.

        table_exists_err_templates = [
            'table %(name)s already exists',
            'table "%(name)s" already exists',
        ]

        def table_exists_errback(f):
            f.trap(OperationalError)
            for err_template in table_exists_err_templates:
                if err_template % {'name': table.name} in str(f.value):
                    return None
            return f

        d = self._conn.execute(CreateTable(table))
        d.addErrback(table_exists_errback)
        return d.addCallback(lambda r: trx)

    def create_tables(self):
        d = self._conn.begin()
        for table in self._metadata.sorted_tables:
            d.addCallback(self._create_table, table)
        return d.addCallback(lambda trx: trx.commit())

    def exists(self):
        # It would be nice to make this not use private things.
        return self._conn._engine.has_table(
            self._metadata.sorted_tables[0].name)

    def execute_query(self, query, *args, **kw):
        def table_missing_errback(f):
            f.trap(OperationalError)
            if 'no such table: ' in str(f.value):
                raise TableMissingError(f.value.args[0])
            return f

        d = self._conn.execute(query, *args, **kw)
        return d.addErrback(table_missing_errback)

    def execute_fetchall(self, query, *args, **kw):
        d = self.execute_query(query, *args, **kw)
        return d.addCallback(lambda result: result.fetchall())
