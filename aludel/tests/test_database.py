import json
import os

from sqlalchemy import (
    Table, Column, Integer, String, UniqueConstraint, MetaData
)
from sqlalchemy.types import UserDefinedType
from twisted.trial.unittest import TestCase

from aludel.database import (
    get_engine, make_table, CollectionMissingError, _PrefixedTables,
    CollectionMetadata, TableCollection,
)

from .doubles import FakeReactorThreads


class DatabaseTestCase(TestCase):
    def setUp(self):
        connection_string = os.environ.get(
            "ALUDEL_TEST_CONNECTION_STRING", "sqlite://")
        self.engine = get_engine(
            connection_string, reactor=FakeReactorThreads())
        self._drop_tables()
        self.conn = self.successResultOf(self.engine.connect())

    def tearDown(self):
        self.successResultOf(self.conn.close())
        self._drop_tables()
        assert self.successResultOf(self.engine.table_names()) == []

    def _drop_tables(self):
        # NOTE: This is a blocking operation!
        md = MetaData(bind=self.engine._engine)
        md.reflect()
        md.drop_all()


class Test_PrefixedTables(DatabaseTestCase):
    def test_get_table_name_not_implemented(self):
        """
        .get_table_name() should raise a NotImplementedError.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        err = self.assertRaises(
            NotImplementedError, my_tables.get_table_name, 'foo')
        assert err.args[0] == "_PrefixedTables should not be used directly."

    def test_exists_not_implemented(self):
        """
        .exists() should raise a NotImplementedError.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        err = self.assertRaises(NotImplementedError, my_tables.exists)
        assert err.args[0] == "_PrefixedTables should not be used directly."

    def test__execute_query_happy(self):
        """
        ._execute_query() should query the database and return a result.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        result = self.successResultOf(my_tables._execute_query("SELECT 42;"))
        rows = self.successResultOf(result.fetchall())
        assert rows == [(42,)]

    def test__execute_error(self):
        """
        ._execute_query() should fail if given an invalid query.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        self.failureResultOf(my_tables._execute_query("SELECT ;;"))

    def test_execute_query_not_implemented(self):
        """
        .execute_query() should raise a NotImplementedError.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        err = self.assertRaises(
            NotImplementedError, my_tables.execute_query, "SELECT 42;")
        assert err.args[0] == "_PrefixedTables should not be used directly."

    def test_execute_fetchall_not_implemented(self):
        """
        .execute_fetchall() should raise a NotImplementedError.
        """
        my_tables = _PrefixedTables("prefix", self.conn)
        err = self.assertRaises(
            NotImplementedError, my_tables.execute_fetchall, "SELECT 42;")
        assert err.args[0] == "_PrefixedTables should not be used directly."


class TestCollectionMetadata(DatabaseTestCase):
    def test_create_new(self):
        """
        .create() should create the appropriately named table.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        has_table_d = self.engine.has_table(cmd.collection_metadata.name)
        assert self.successResultOf(has_table_d) is False
        assert self.successResultOf(cmd.exists()) is False

        self.successResultOf(cmd.create())
        has_table_d = self.engine.has_table(cmd.collection_metadata.name)
        assert self.successResultOf(has_table_d) is True
        assert self.successResultOf(cmd.exists()) is True

    def test_create_exists(self):
        """
        .create() should do nothing if the table already exists.
        """
        cmd = CollectionMetadata('MyTables', self.conn)

        self.successResultOf(cmd.create())
        has_table_d = self.engine.has_table(cmd.collection_metadata.name)
        assert self.successResultOf(has_table_d) is True
        assert self.successResultOf(cmd.exists()) is True

        # Create again, assert that everything still exists.
        self.successResultOf(cmd.create())
        has_table_d = self.engine.has_table(cmd.collection_metadata.name)
        assert self.successResultOf(has_table_d) is True
        assert self.successResultOf(cmd.exists()) is True

    def test_collection_exists_no_table(self):
        """
        .collection_exists() should return None if the metadata table does not
        exist.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        assert self.successResultOf(cmd.collection_exists('foo')) is None

    def test_collection_exists_no_metadata(self):
        """
        .collection_exists() should return False if there is no metadata for
        the provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        assert self.successResultOf(cmd.collection_exists('foo')) is False

    def test_collection_exists_with_metadata(self):
        """
        .collection_exists() should return True if there is metadata for the
        provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo', {'bar': 'baz'}))
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}
        assert self.successResultOf(cmd.collection_exists('foo')) is True

    def test_get_metadata_no_table(self):
        """
        .get_metadata() should fail with CollectionMissingError if the metadata
        table does not exist.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.failureResultOf(cmd.get_metadata('foo'), CollectionMissingError)

    def test_get_metadata_missing_collection(self):
        """
        .get_metadata() should fail with CollectionMissingError if there is no
        metadata for the provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.failureResultOf(cmd.get_metadata('foo'), CollectionMissingError)

    def test_get_metadata_from_cache(self):
        """
        .get_metadata() should return metadata from the local cache if present.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        cmd._metadata_cache['foo'] = json.dumps({'bar': 'baz'})
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}

    def test_get_metadata_no_cache(self):
        """
        .get_metadata() should populate the local cache entry from the database
        if necessary.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo', {'bar': 'baz'}))
        cmd._metadata_cache.pop('foo')
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}
        assert cmd._metadata_cache['foo'] == json.dumps({'bar': 'baz'})

    def test_get_all_metadata_no_cache(self):
        """
        .get_all_metadata() should populate the local cache from the database
        and return a copy of all the metadata.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo', {'a': 1}))
        self.successResultOf(cmd.create_collection('bar', {'b': 2}))
        cmd._metadata_cache.clear()
        metadata = self.successResultOf(cmd.get_all_metadata())
        assert metadata == {'foo': {'a': 1}, 'bar': {'b': 2}}
        assert cmd._metadata_cache == dict(
            (k, json.dumps(v)) for k, v in metadata.iteritems())

    def test_get_all_metadata_extra_cache(self):
        """
        .get_all_metadata() should remove extra entries from the local cache.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo', {'a': 1}))
        self.successResultOf(cmd.create_collection('bar', {'b': 2}))
        cmd._metadata_cache['baz'] = json.dumps({'c': 3})
        metadata = self.successResultOf(cmd.get_all_metadata())
        assert metadata == {'foo': {'a': 1}, 'bar': {'b': 2}}
        assert cmd._metadata_cache == dict(
            (k, json.dumps(v)) for k, v in metadata.iteritems())

    def test__decode_all_metadata_with_none(self):
        """
        ._decode_all_metadata() should ignore empty metadata entries.
        """
        cmd = CollectionMetadata('MyTables', None)
        metadata_cache = {'foo': json.dumps({'a': 1}), 'bar': None}
        assert cmd._decode_all_metadata(metadata_cache) == {'foo': {'a': 1}}

    def test_set_metadata(self):
        """
        .set_metadata() should update the database and the local cache.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo'))
        assert cmd._metadata_cache['foo'] == json.dumps({})
        self.successResultOf(cmd.set_metadata('foo', {'bar': 'baz'}))
        assert cmd._metadata_cache['foo'] == json.dumps({'bar': 'baz'})
        # Clear the local cache and assert that the new version is fetched from
        # the db.
        cmd._metadata_cache.pop('foo')
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}

    def test_create_collection_no_table(self):
        """
        .create_collection() should call .create() before creating the
        collection if the metadata table does not exist.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create_collection('foo'))
        assert cmd._metadata_cache['foo'] == json.dumps({})

    def test_create_collection_no_metadata(self):
        """
        .create_collection() should create a collection metadata entry with an
        empty dict if no metadata is provided.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo'))
        assert cmd._metadata_cache['foo'] == json.dumps({})

    def test_create_collection_with_metadata(self):
        """
        .create_collection() should create a collection metadata entry with the
        provided metadata.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo', {'bar': 'baz'}))
        assert cmd._metadata_cache['foo'] == json.dumps({'bar': 'baz'})


class TestTableCollection(DatabaseTestCase):
    def _get_cmd(self, collection_cls):
        """
        Create and return a CollectionMetadata instance for collection_cls.
        """
        cmd = CollectionMetadata(collection_cls.collection_type(), self.conn)
        self.successResultOf(cmd.create())
        return cmd

    def test_collection_type_class_name(self):
        """
        .collection_type() should return the class name if the COLLECTION_TYPE
        attr is unset.
        """
        class MyTables(TableCollection):
            pass

        assert MyTables.collection_type() == 'MyTables'
        my_tables = MyTables("prefix", connection=None)
        assert my_tables.collection_type() == 'MyTables'

    def test_collection_type_explicit_name(self):
        """
        .collection_type() should return the COLLECTION_TYPE attr if set.
        """
        class MyTables(TableCollection):
            COLLECTION_TYPE = 'YourTables'

        assert MyTables.collection_type() == 'YourTables'
        my_tables = MyTables("prefix", connection=None)
        assert my_tables.collection_type() == 'YourTables'

    def test_init_uses_provided_collection_metadata(self):
        """
        TableCollection should use the collection_metadata it's given, if any.
        """
        cmd = self._get_cmd(TableCollection)
        my_tables = TableCollection("foo", None, collection_metadata=cmd)
        assert my_tables._collection_metadata is cmd

    def test_init_uses_builds_collection_metadata(self):
        """
        TableCollection should build a collection_metadata if none is given.
        """
        my_tables = TableCollection("foo", None)
        assert isinstance(my_tables._collection_metadata, CollectionMetadata)

    def test_get_table_name(self):
        """
        .get_table_name() should build an appropriate table name from the
        collection type, collection name, and table name.
        """
        class MyTables(TableCollection):
            pass

        my_tables = MyTables("prefix", connection=None)
        assert my_tables.get_table_name("thing") == "MyTables_prefix_thing"

    def test_make_table(self):
        """
        Class attributes built by make_table() should be replaced by instance
        attributes that are SQLAlchemy Table instances with the correct table
        names.
        """
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
                Column("other_value", String(255)),
                UniqueConstraint("value", "other_value"),
            )

        my_tables_1 = MyTables("prefix1", self.conn)
        assert isinstance(my_tables_1.tbl, Table)
        assert my_tables_1.tbl.name == 'MyTables_prefix1_tbl'
        assert len(my_tables_1.tbl.c) == 3

        # Make another instance to check that things aren't bound improperly.
        my_tables_2 = MyTables("prefix2", self.conn)
        assert isinstance(my_tables_2.tbl, Table)
        assert my_tables_2.tbl.name == 'MyTables_prefix2_tbl'
        assert len(my_tables_2.tbl.c) == 3

    def test_create_tables_with_metadata(self):
        """
        .create_tables() should create the tables belonging to the collection
        and set metadata.
        """
        class MyTables(TableCollection):
            tbl1 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
            )

            tbl2 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("other_value", String(255)),
            )

        cmd = self._get_cmd(MyTables)
        my_tables = MyTables("foo", self.conn, cmd)

        # Check that the tables don't already exist.
        assert self.successResultOf(my_tables.exists()) is False
        self.failureResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.failureResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables and check that they exist.
        self.successResultOf(my_tables.create_tables(metadata={'bar': 'baz'}))
        assert self.successResultOf(my_tables.exists()) is True
        self.successResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.successResultOf(self.conn.execute(my_tables.tbl2.select()))
        assert self.successResultOf(cmd.get_metadata("foo")) == {'bar': 'baz'}

    def test_create_tables_no_metadata(self):
        """
        .create_tables() should create the tables belonging to the collection
        and set metadata. If no metadata is provided, an empty dict should be
        used.
        """
        class MyTables(TableCollection):
            tbl1 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
            )

            tbl2 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("other_value", String(255)),
            )

        cmd = self._get_cmd(MyTables)
        my_tables = MyTables("foo", self.conn, cmd)

        # Check that the tables don't already exist.
        assert self.successResultOf(my_tables.exists()) is False
        self.failureResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.failureResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables and check that they exist.
        self.successResultOf(my_tables.create_tables())
        assert self.successResultOf(my_tables.exists()) is True
        self.successResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.successResultOf(self.conn.execute(my_tables.tbl2.select()))
        assert self.successResultOf(cmd.get_metadata("foo")) == {}

    def test_create_tables_already_exists(self):
        """
        .create_tables() should do nothing if the tables already exist.
        """
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
            )

        cmd = self._get_cmd(MyTables)
        my_tables = MyTables("foo", self.conn, cmd)

        # Create the tables and check that they exist.
        self.successResultOf(my_tables.create_tables(metadata={'bar': 'baz'}))
        assert self.successResultOf(my_tables.exists()) is True
        assert self.successResultOf(cmd.get_metadata("foo")) == {'bar': 'baz'}

        # Create the tables again and check that nothing changes.
        self.successResultOf(my_tables.create_tables(metadata={'a': 'b'}))
        assert self.successResultOf(my_tables.exists()) is True
        assert self.successResultOf(cmd.get_metadata("foo")) == {'bar': 'baz'}

    def test_create_tables_error(self):
        """
        .create_tables() should fail if the tables can't be created.
        """
        class BrokenType(UserDefinedType):
            def get_col_spec(self):
                return "BROKEN;;"

        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", BrokenType()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.create_tables())

    def test_get_metadata(self):
        """
        .get_metadata() should fetch the metadata for this collection.
        """
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
            )

        my_tables = MyTables("prefix", self.conn)
        self.successResultOf(my_tables._collection_metadata.create())
        self.successResultOf(my_tables.create_tables(metadata={'bar': 'baz'}))

        assert self.successResultOf(my_tables.get_metadata()) == {'bar': 'baz'}

    def test_set_metadata(self):
        """
        .set_metadata() should update the metadata for this collection.
        """
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String(255)),
            )

        my_tables = MyTables("prefix", self.conn)
        self.successResultOf(my_tables._collection_metadata.create())
        self.successResultOf(my_tables.create_tables())

        assert self.successResultOf(my_tables.get_metadata()) == {}
        self.successResultOf(my_tables.set_metadata({'bar': 'baz'}))
        assert self.successResultOf(my_tables.get_metadata()) == {'bar': 'baz'}

    def test_execute_query_happy(self):
        """
        .execute_query() should query the database and return a result.
        """
        my_tables = TableCollection("prefix", self.conn)
        self.successResultOf(my_tables.create_tables())
        result = self.successResultOf(my_tables.execute_query("SELECT 42;"))
        rows = self.successResultOf(result.fetchall())
        assert rows == [(42,)]

    def test_execute_query_no_collection(self):
        """
        .execute_query() should fail with CollectionMissingError if the
        collection does not exist.
        """
        my_tables = TableCollection("prefix", self.conn)
        self.failureResultOf(
            my_tables.execute_query("SELECT 42;"), CollectionMissingError)

    def test_execute_query_error(self):
        """
        .execute_query() should fail if given an invalid query.
        """
        my_tables = TableCollection("prefix", self.conn)
        self.successResultOf(my_tables.create_tables())
        self.failureResultOf(my_tables.execute_query("SELECT ;;"))

    def test_execute_fetchall_no_collection(self):
        """
        .execute_fetchall() should fail with CollectionMissingError if the
        collection does not exist.
        """
        my_tables = TableCollection("prefix", self.conn)
        self.failureResultOf(
            my_tables.execute_fetchall("SELECT 42;"), CollectionMissingError)

    def test_execute_fetchall(self):
        """
        .execute_fetchall() should query the database and return all rows from
        the result.
        """
        my_tables = TableCollection("prefix", self.conn)
        self.successResultOf(my_tables.create_tables())
        rows = self.successResultOf(my_tables.execute_fetchall("SELECT 42;"))
        assert rows == [(42,)]
