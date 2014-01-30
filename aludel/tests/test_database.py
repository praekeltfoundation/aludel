from sqlalchemy import Table, Column, Integer, String, UniqueConstraint
from sqlalchemy.types import UserDefinedType

from twisted.trial.unittest import TestCase

from aludel.database import (
    get_engine, make_table, CollectionMetadata, TableCollection,
    CollectionMissingError, TableMissingError,
)

from .doubles import FakeReactorThreads


class TestCollectionMetadata(TestCase):
    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())

    def tearDown(self):
        self.successResultOf(self.conn.close())

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

    def test_collection_exists_no_metadata(self):
        """
        .collection_exists() should return False if there is no metadata for
        the provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        assert self.successResultOf(cmd.collection_exists('foo')) is False
        self.successResultOf(cmd.create_collection('foo'))
        assert self.successResultOf(cmd.collection_exists('foo')) is True
        assert self.successResultOf(cmd.get_metadata('foo')) == {}

    def test_collection_exists_with_metadata(self):
        """
        .collection_exists() should return True if there is metadata for the
        provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        assert self.successResultOf(cmd.collection_exists('foo')) is False
        self.successResultOf(cmd.create_collection('foo', {'bar': 'baz'}))
        assert self.successResultOf(cmd.collection_exists('foo')) is True
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}

    def test_get_metadata_missing_collection(self):
        """
        .get_metadata() should fail with CollectionMissingError if there is no
        metadata for the provided name.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.assertFailure(cmd.get_metadata('foo'), CollectionMissingError)

    def test_get_metadata_from_cache(self):
        """
        .get_metadata() should return metadata from the local cache if present.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        cmd._metadata_cache['foo'] = {'bar': 'baz'}
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
        assert cmd._metadata_cache['foo'] == {'bar': 'baz'}

    def test_set_metadata(self):
        """
        .set_metadata() should update the database and the local cache.
        """
        cmd = CollectionMetadata('MyTables', self.conn)
        self.successResultOf(cmd.create())
        self.successResultOf(cmd.create_collection('foo'))
        assert cmd._metadata_cache['foo'] == {}
        self.successResultOf(cmd.set_metadata('foo', {'bar': 'baz'}))
        assert cmd._metadata_cache['foo'] == {'bar': 'baz'}
        # Clear the local cache and assert that the new version is fetched from
        # the db.
        cmd._metadata_cache.pop('foo')
        assert self.successResultOf(cmd.get_metadata('foo')) == {'bar': 'baz'}


class TestTableCollection(TestCase):
    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())

    def tearDown(self):
        self.successResultOf(self.conn.close())

    def test_collection_type_class_name(self):
        class MyTables(TableCollection):
            pass

        assert MyTables.collection_type() == 'MyTables'
        my_tables = MyTables("prefix", connection=None)
        assert my_tables.collection_type() == 'MyTables'

    def test_collection_type_explicit_name(self):
        class MyTables(TableCollection):
            COLLECTION_TYPE = 'YourTables'

        assert MyTables.collection_type() == 'YourTables'
        my_tables = MyTables("prefix", connection=None)
        assert my_tables.collection_type() == 'YourTables'

    def test_get_table_name(self):
        class MyTables(TableCollection):
            pass

        my_tables = MyTables("prefix", connection=None)
        assert my_tables.get_table_name("thing") == "MyTables_prefix_thing"

    def test_make_table(self):
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
                Column("other_value", String()),
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

    def test_create_tables(self):
        class MyTables(TableCollection):
            tbl1 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

            tbl2 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("other_value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.successResultOf(my_tables._collection_metadata.create())

        # Check that the tables don't already exist.
        assert self.successResultOf(my_tables.exists()) is False
        self.failureResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.failureResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables and check that they exist.
        self.successResultOf(my_tables.create_tables())
        assert self.successResultOf(my_tables.exists()) is True
        self.successResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.successResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables again and check that nothing explodes.
        self.successResultOf(my_tables.create_tables())

    def test_create_tables_error(self):
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

    def test_execute_happy(self):
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        result = self.successResultOf(my_tables.execute_query("SELECT 42;"))
        rows = self.successResultOf(result.fetchall())
        assert rows == [(42,)]

    def test_execute_no_table(self):
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.execute_query(
            my_tables.tbl.select()), TableMissingError)

    def test_execute_error(self):
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.execute_query("SELECT ;;"))

    def test_execute_fetchall(self):
        class MyTables(TableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        rows = self.successResultOf(my_tables.execute_fetchall("SELECT 42;"))
        assert rows == [(42,)]
