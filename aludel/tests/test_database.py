from sqlalchemy import Table, Column, Integer, String, UniqueConstraint
from sqlalchemy.types import UserDefinedType

from twisted.trial.unittest import TestCase

from aludel.database import (
    get_engine, make_table, PrefixedTableCollection, TableMissingError,
)

from .doubles import FakeReactorThreads


class TestPrefixedTableCollection(TestCase):
    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())

    def tearDown(self):
        self.successResultOf(self.conn.close())

    def test_get_table_name(self):
        class MyTables(PrefixedTableCollection):
            pass

        my_tables = MyTables("prefix", connection=None)
        assert my_tables.get_table_name("thing") == "prefix_thing"

    def test_make_table(self):
        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
                Column("other_value", String()),
                UniqueConstraint("value", "other_value"),
            )

        my_tables_1 = MyTables("prefix1", self.conn)
        assert isinstance(my_tables_1.tbl, Table)
        assert my_tables_1.tbl.name == 'prefix1_tbl'
        assert len(my_tables_1.tbl.c) == 3

        # Make another instance to check that things aren't bound improperly.
        my_tables_2 = MyTables("prefix2", self.conn)
        assert isinstance(my_tables_2.tbl, Table)
        assert my_tables_2.tbl.name == 'prefix2_tbl'
        assert len(my_tables_2.tbl.c) == 3

    def test_create_tables(self):
        class MyTables(PrefixedTableCollection):
            tbl1 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

            tbl2 = make_table(
                Column("id", Integer(), primary_key=True),
                Column("other_value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        # Check that the tables don't already exist.
        assert not self.successResultOf(my_tables.exists())
        self.failureResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.failureResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables and check that they exist.
        self.successResultOf(my_tables.create_tables())
        assert self.successResultOf(my_tables.exists())
        self.successResultOf(self.conn.execute(my_tables.tbl1.select()))
        self.successResultOf(self.conn.execute(my_tables.tbl2.select()))

        # Create the tables again and check that nothing explodes.
        self.successResultOf(my_tables.create_tables())

    def test_create_tables_error(self):
        class BrokenType(UserDefinedType):
            def get_col_spec(self):
                return "BROKEN;;"

        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", BrokenType()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.create_tables())

    def test_execute_happy(self):
        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        result = self.successResultOf(my_tables.execute_query("SELECT 42;"))
        rows = self.successResultOf(result.fetchall())
        assert rows == [(42,)]

    def test_execute_no_table(self):
        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.execute_query(
            my_tables.tbl.select()), TableMissingError)

    def test_execute_error(self):
        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        self.failureResultOf(my_tables.execute_query("SELECT ;;"))

    def test_execute_fetchall(self):
        class MyTables(PrefixedTableCollection):
            tbl = make_table(
                Column("id", Integer(), primary_key=True),
                Column("value", String()),
            )

        my_tables = MyTables("prefix", self.conn)
        rows = self.successResultOf(my_tables.execute_fetchall("SELECT 42;"))
        assert rows == [(42,)]
