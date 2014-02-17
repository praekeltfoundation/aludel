import json

from alchimia import TWISTED_STRATEGY
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine
from sqlalchemy.schema import CreateTable
from twisted.internet.defer import succeed


def get_engine(conn_str, reactor):
    return create_engine(conn_str, reactor=reactor, strategy=TWISTED_STRATEGY)


class TableMissingError(Exception):
    """
    Raised when a table does not exist in the database.
    """


class CollectionMissingError(Exception):
    """
    Raised when no metadata was found for a :class:`TableCollection`.
    """


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


def _false_to_error(result, err):
    if not result:
        raise err
    return result


TABLE_EXISTS_ERR_TEMPLATES = (
    # SQLite
    'table %(name)s already exists',
    'table "%(name)s" already exists',
    # PostgreSQL
    'relation %(name)s already exists',
    'relation "%(name)s" already exists',
    # MySQL
    'Table %(name)s already exists',
    "Table '%(name)s' already exists",
)


class _PrefixedTables(object):
    def __init__(self, name, connection):
        self.name = name
        self._conn = connection
        self._metadata = MetaData()
        for attr in dir(self):
            attrval = getattr(self, attr)
            if isinstance(attrval, make_table):
                setattr(self, attr, attrval.make_table(
                    self.get_table_name(attr), self._metadata))

    def get_table_name(self, name):
        raise NotImplementedError(
            "_PrefixedTables should not be used directly.")

    def _create_table(self, trx, table):
        # This works around alchimia's current inability to create tables only
        # if they don't already exist.

        def table_exists_errback(f):
            for err_template in TABLE_EXISTS_ERR_TEMPLATES:
                # Sometimes the table name is lowercased.
                for name in (table.name, table.name.lower()):
                    if err_template % {'name': name} in str(f.value):
                        return None
            return f

        d = self._conn.execute(CreateTable(table))
        d.addErrback(table_exists_errback)
        return d.addCallback(lambda r: trx)

    def _create_tables(self):
        d = self._conn.begin()
        for table in self._metadata.sorted_tables:
            d.addCallback(self._create_table, table)
        return d.addCallback(lambda trx: trx.commit())

    def exists(self):
        raise NotImplementedError(
            "_PrefixedTables should not be used directly.")

    def _execute_query(self, query, *args, **kw):
        return self._conn.execute(query, *args, **kw)

    def execute_query(self, query, *args, **kw):
        raise NotImplementedError(
            "_PrefixedTables should not be used directly.")

    def execute_fetchall(self, query, *args, **kw):
        d = self.execute_query(query, *args, **kw)
        return d.addCallback(lambda result: result.fetchall())


class CollectionMetadata(_PrefixedTables):
    """
    Metadata manager for PrefixedTableCollection.

    This tracks table prefixes and metadata for a given collection type.
    """

    collection_metadata = make_table(
        Column("name", String(255), primary_key=True),
        Column("metadata_json", Text(), nullable=False),
    )

    _existence_cache_dict = None

    @property
    def _existence_cache(self):
        if self._existence_cache_dict is None:
            self._existence_cache_dict = {}
        return self._existence_cache_dict

    def get_table_name(self, name):
        return '%s_%s' % (name, self.name)

    def exists(self):
        # It would be nice to make this not use private things.
        return self._conn._engine.has_table(self.collection_metadata.name)

    def create(self):
        return self._create_tables()

    def execute_query(self, query, *args, **kw):
        d = self.exists()
        d.addCallback(
            _false_to_error, TableMissingError(self.collection_metadata.name))
        d.addCallback(lambda _: self._execute_query(query, *args, **kw))
        return d

    def _update_existence_cache(self, new_metadata, clear=False):
        cache = self._existence_cache
        if clear:
            cache.clear()
        cache.update(dict((k, False if v is None else True)
                          for k, v in new_metadata.iteritems()))
        # We return this so we can chain callbacks.
        return new_metadata

    def _rows_to_dict(self, rows):
        metadata_dict = {}
        for name, metadata_json in rows:
            metadata_dict[name] = metadata_json
        return metadata_dict

    def _add_row_to_metadata(self, row, name):
        metadata_json = None
        if row is not None:
            metadata_json = row.metadata_json
        self._update_existence_cache({name: metadata_json})
        return metadata_json

    def _none_if_table_missing_eb(self, failure):
        failure.trap(TableMissingError)
        return None

    def _decode_metadata(self, metadata_json, name):
        if metadata_json is None:
            raise CollectionMissingError(name)
        return json.loads(metadata_json)

    def _get_metadata(self, name):
        d = self.execute_query(
            self.collection_metadata.select().where(
                self.collection_metadata.c.name == name))
        d.addCallback(lambda result: result.fetchone())
        d.addCallback(self._add_row_to_metadata, name)
        return d

    def get_metadata(self, name):
        d = self._get_metadata(name)
        d.addErrback(self._none_if_table_missing_eb)
        d.addCallback(self._decode_metadata, name)
        return d

    def _decode_all_metadata(self, all_metadata):
        metadata = {}
        for name, metadata_json in all_metadata.iteritems():
            if metadata_json is not None:
                metadata[name] = json.loads(metadata_json)
        return metadata

    def get_all_metadata(self):
        d = self.execute_fetchall(self.collection_metadata.select())
        d.addCallback(self._rows_to_dict)
        d.addCallback(self._update_existence_cache, clear=True)
        d.addCallback(self._decode_all_metadata)
        return d

    def set_metadata(self, name, metadata):
        metadata_json = json.dumps(metadata)
        d = self.execute_query(
            self.collection_metadata.update().where(
                self.collection_metadata.c.name == name,
            ).values(metadata_json=metadata_json))
        d.addCallback(lambda result: {name: metadata_json})
        d.addCallback(self._update_existence_cache)
        return d

    def _create_collection(self, exists, name, metadata):
        metadata_json = json.dumps(metadata)
        if exists:
            return
        if exists is None:
            d = self.create()
        else:
            d = succeed(None)

        d.addCallback(lambda _: self.execute_query(
            self.collection_metadata.insert().values(
                name=name, metadata_json=metadata_json)))
        d.addCallback(lambda result: {name: metadata_json})
        d.addCallback(self._update_existence_cache)
        return d

    def create_collection(self, name, metadata=None):
        """
        Create a metadata entry for the named collection.

        :param str name: Name of the collection to check.
        :param dict metadata:
            Metadata value to store. If ``None``, an empty dict will be used.

        If the metadata table does not exist, :meth:`CollectionMetadata.create`
        will be called first.
        """
        if metadata is None:
            metadata = {}
        d = self.collection_exists(name)
        d.addCallback(self._create_collection, name, metadata)
        return d

    def collection_exists(self, name):
        """
        Check for the existence of the named collection.

        :param str name: Name of the collection to check.

        If there is a metadata entry for ``name``, ``True`` is returned. If
        there is no metadata entry, ``False`` is returned. If the metadata
        table does not exist, ``None`` is returned. Both ``False`` and ``None``
        are truthless values and the difference may be important to the caller.

        :returns:
            A :class:`Deferred` that fires with ``True``, ``False``, or
            ``None``.
        """
        d = succeed(name)
        if name not in self._existence_cache:
            d.addCallback(self._get_metadata)
        d.addCallback(lambda _: self._existence_cache[name])
        d.addErrback(self._none_if_table_missing_eb)
        return d


class TableCollection(_PrefixedTables):
    """
    Collection of database tables sharing a common prefix.

    Each table is prefixed with the collection type and name.

    The collection type defaults to the class name, but the
    :attr:`COLLECTION_TYPE` class attribute may be set to override this.
    """

    COLLECTION_TYPE = None

    def __init__(self, name, connection, collection_metadata=None):
        super(TableCollection, self).__init__(name, connection)
        if collection_metadata is None:
            collection_metadata = CollectionMetadata(
                self.collection_type(), connection)
        self._collection_metadata = collection_metadata

    @classmethod
    def collection_type(cls):
        ctype = cls.COLLECTION_TYPE
        if ctype is None:
            ctype = cls.__name__
        return ctype

    def get_table_name(self, name):
        return '%s_%s_%s' % (self.collection_type(), self.name, name)

    def exists(self):
        return self._collection_metadata.collection_exists(self.name)

    def create_tables(self, metadata=None):
        d = self._create_tables()
        d.addCallback(lambda _: self._collection_metadata.create_collection(
            self.name, metadata))
        return d

    def get_metadata(self):
        return self._collection_metadata.get_metadata(self.name)

    def set_metadata(self, metadata):
        return self._collection_metadata.set_metadata(self.name, metadata)

    def execute_query(self, query, *args, **kw):
        d = self.exists()
        d.addCallback(_false_to_error, CollectionMissingError(self.name))
        d.addCallback(lambda _: self._execute_query(query, *args, **kw))
        return d
