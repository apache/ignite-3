# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import decimal
import uuid
import pkgutil
from typing import Optional, List, Any, Sequence, Union

from pyignite_dbapi import _pyignite_dbapi_extension
from pyignite_dbapi import native_type_code

__version__ = pkgutil.get_data(__name__, "_version.txt").decode

apilevel = '2.0'
"""PEP 249 is supported."""

threadsafety = 2
"""Threads may share the module and connections, but not cursors."""

paramstyle = 'qmark'
"""Parameter style is a question mark, e.g. '...WHERE name=?'."""

NULL = None
"""Null constant."""

BOOLEAN = bool
"""Boolean type."""

INT = int
"""Integer type."""

FLOAT = float
"""Floating point type."""

STRING = str
"""String type."""

BINARY = bytes
"""Binary type."""

NUMBER = decimal.Decimal
"""Big number (Decimal) type."""

DATE = datetime.date
"""Date type."""

TIME = datetime.time
"""Time type."""

DATETIME = datetime.datetime
"""Date-Time type."""

DURATION = datetime.timedelta
"""Duration type."""

UUID = uuid.UUID
"""UUID type."""

ROWID = UUID
"""Type object that is used to describe the “Row ID” column in a database."""


class TIMESTAMP(float):
    """
    Timestamp data type.
    """
    pass


# noinspection PyPep8Naming
def Date(year, month, day):
    """
    Function that is used to construct an object holding a date value.
    """
    return DATE(year=year, month=month, day=day)


# noinspection PyPep8Naming
def Time(hour, minute, second):
    """
    Function that is used to construct an object holding a time value.
    """
    return TIME(hour=hour, minute=minute, second=second)


# noinspection PyPep8Naming
def Timestamp(year, month, day, hour, minute, second):
    """
    Function that is used to construct an object holding a time stamp value.
    """
    dt = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
    ts = dt.timestamp()
    return TIMESTAMP(ts)


# noinspection PyPep8Naming
def DateFromTicks(ticks):
    """
    Function used to construct an object holding a date value from the given ticks value (number of seconds
    since the epoch; see the documentation of the standard Python time module for details).
    """
    return DATE.fromtimestamp(ticks)


# noinspection PyPep8Naming
def TimeFromTicks(ticks):
    """
    Function used to construct an object holding a time value from the given ticks value (number of seconds
    since the epoch; see the documentation of the standard Python time module for details).
    """
    return DATETIME.fromtimestamp(ticks).time()


# noinspection PyPep8Naming
def TimestampFromTicks(ticks):
    """
    Function used to construct an object holding a time stamp value from the given ticks value (number of
    seconds since the epoch; see the documentation of the standard Python time module for details).
    """
    return TIMESTAMP(ticks)


# noinspection PyPep8Naming
def Binary(string: Union[str, bytes]):
    """
    Function that is used to construct an object capable of holding a binary (long) string value.
    """
    if isinstance(string, bytes):
        return string
    else:
        return BINARY(string, 'utf-8')


class Error(Exception):
    """
    Exception that is the base class of all other error exceptions. You can use this exception to catch all errors in
    one except statement. Warnings are not considered errors and thus should not use this class as base.
    """
    pass


# noinspection PyShadowingBuiltins
class Warning(Exception):
    """
    Exception that is raised for important warnings like data truncations while inserting, etc.
    """
    pass


class InterfaceError(Error):
    """
    Exception that is raised for errors that are related to the database interface rather than the database itself.
    """
    pass


class DatabaseError(Error):
    """
    Exception that is raised for errors that are related to the database (for example, the database being unavailable).
    """
    pass


class DataError(DatabaseError):
    """
    Exception that is raised for errors that are caused by problems with data (for example, division by zero, numeric
    value out of range, etc.).
    """
    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error, e.g. the cursor is not valid anymore, the
    transaction is out of sync, etc.
    """
    pass


class OperationalError(DatabaseError):
    """
    Exception that is raised for errors that are related to the database’s operation and not necessarily under the
    control of the programmer, e.g. an unexpected disconnect occurs, the data source name is not found, a transaction
    could not be processed, a memory allocation error occurred during processing, etc.
    """
    pass


class ProgrammingError(DatabaseError):
    """
    Exception that is raised for programming errors, e.g. table not found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.
    """
    pass


class IntegrityError(DatabaseError):
    """
    Exception that is raised when the relational integrity of the database is affected (for example, a foreign key check
    fails).
    """
    pass


class NotSupportedError(DatabaseError):
    """
    Exception that is raised when an unsupported method or database API was used.
    """
    pass


def _type_code_from_int(native: int):
    if native == native_type_code.NIL:
        return NULL
    elif native == native_type_code.BOOLEAN:
        return BOOLEAN
    elif (native == native_type_code.INT8 or native == native_type_code.INT16
          or native == native_type_code.INT32 or native == native_type_code.INT64):
        return INT
    elif native == native_type_code.FLOAT or native == native_type_code.DOUBLE:
        return FLOAT
    elif native == native_type_code.DECIMAL or native == native_type_code.NUMBER:
        return NUMBER
    elif native == native_type_code.DATE:
        return DATE
    elif native == native_type_code.TIME:
        return TIME
    elif native == native_type_code.DATETIME or native == native_type_code.TIMESTAMP:
        return DATETIME
    elif native == native_type_code.UUID:
        return UUID
    elif native == native_type_code.BITMASK:
        return INT
    elif native == native_type_code.STRING:
        return STRING
    elif native == native_type_code.BYTE_ARRAY:
        return BINARY
    elif native == native_type_code.PERIOD or native == native_type_code.DURATION:
        return DATETIME
    raise InterfaceError(f'Unsupported data type: {native}')


class ColumnDescription(tuple):
    """
    Represents a description of the result set's single column.
    """

    def __new__(cls, name: str, type_code: int, display_size: Optional[int], internal_size: Optional[int],
                precision: Optional[int], scale: Optional[int], null_ok: bool):
        self = tuple.__new__(
            cls, (name, _type_code_from_int(type_code), display_size, internal_size, precision, scale, null_ok)
        )

        self.name = self[0]
        self.type_code = self[1]
        self.display_size = self[2]
        self.internal_size = self[3]
        self.precision = self[4]
        self.scale = self[5]
        self.null_ok = self[6]

        return self


class Cursor:
    """
    Cursor class. Represents a single statement and holds the result of its execution.

    Attributes
    ----------
    arraysize: int
        Attribute that is used to specify the maximum number of rows that can be fetched at a time with the .fetchmany()
        method. Default value: 1 (single row).
    """
    arraysize: int = 1

    def __init__(self, cur_id: int, py_cursor, conn):
        self._cur_id = cur_id
        self._py_cursor = py_cursor
        self._description = None
        self._rownumber = None
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        """
        Return self to make cursors compatible to the iteration protocol
        """
        return self

    def __next__(self) -> Sequence[Optional[Any]]:
        """
        Return the next row to make cursors compatible to the iteration protocol.
        """
        return self.next()

    def next(self) -> Sequence[Optional[Any]]:
        """
        Return the next row from the currently executing SQL statement by using the same semantics as .fetchone().
        A StopIteration exception is raised when the result set is exhausted.
        """
        res = self.fetchone()
        if res is None:
            raise StopIteration
        return res

    @property
    def description(self) -> Optional[List[ColumnDescription]]:
        """
        Read-only attribute that is a sequence of 7-item sequences.
        Each of these sequences contains information describing one result column. The following sequences are stored:
        - name
        - type_code
        - display_size
        - internal_size
        - precision
        - scale
        - null_ok

        The first two items (name and type_code) are mandatory, the other five are optional and are set to None if
        no meaningful values can be provided.
        This attribute will be None for operations that do not return rows or if the cursor did not have an operation
        invoked via the .execute() method yet.
        """
        if self._py_cursor is None or self._is_non_sql_query():
            return None
        return self._description

    def _is_non_sql_query(self):
        """
        A little hack to detect DDL and DML queries.
        """
        return len(self._description) == 1 and self._description[0].name in ['APPLIED', 'ROWCOUNT']

    @property
    def rowcount(self) -> int:
        """
        Read-only attribute that specifies the number of rows that the last .execute() produced (for DQL statements
        like SELECT) or affected (for DML statements like UPDATE or INSERT).
        The attribute value is -1 if no .execute() has been performed on the cursor or the rowcount of the last
        operation cannot be determined by the interface.
        """
        if self._py_cursor is None:
            return -1
        return self._py_cursor.rowcount()

    @property
    def rownumber(self) -> Optional[int]:
        """
        Read-only attribute that provides the current 0-based index of the cursor in the result set or None if the index
        cannot be determined.
        The index can be seen as index of the cursor in a sequence (the result set). The next fetch operation will fetch
        the row indexed by .rownumber in that sequence.
        """
        return self._rownumber

    @property
    def connection(self):
        """
        Read-only attribute that returns a reference to the Connection object on which the cursor was created.
        """
        return self._conn

    @property
    def lastrowid(self):
        """
        Read-only attribute that provides the rowid of the last modified row.
        As Ignite does not support rowids, this attribute is always set to None.
        """
        return None

    def callproc(self, *_args):
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        raise NotSupportedError('Stored procedures are not supported')

    def close(self):
        """
        Close the active cursor.
        Completes without errors on successfully closed cursors.
        """
        if self._py_cursor is not None:
            self._py_cursor.close()
            self._py_cursor = None
            self._rownumber = None
            # noinspection PyProtectedMember
            self._conn._cursor_closed(self._cur_id)

    def execute(self, query: str, params: Optional[Sequence[Any]] = None):
        """
        Execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
        Arguments are specified as a question mark '?' in the request.

        The parameters may also be specified as a list of tuples to e.g. insert multiple rows in a single operation,
        but this kind of usage is deprecated: .executemany() should be used instead.
        """
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        self._py_cursor.execute(query, params)
        self._update_description()
        self._rownumber = 0

    def _update_description(self):
        """
        Internal method.
        Update column description for the current cursor. To be called after query execution.
        """
        column_num = self._py_cursor.column_count()
        if not column_num:
            self._description = None
            return

        self._description = []
        for column_id in range(self._py_cursor.column_count()):
            self._description.append(ColumnDescription(
                name=self._py_cursor.column_name(column_id),
                type_code=self._py_cursor.column_type_code(column_id),
                display_size=self._py_cursor.column_display_size(column_id),
                internal_size=self._py_cursor.column_internal_size(column_id),
                precision=self._py_cursor.column_precision(column_id),
                scale=self._py_cursor.column_scale(column_id),
                null_ok=self._py_cursor.column_null_ok(column_id)
            ))

    def executemany(self, query: str, params_list: List[Sequence[Any]]):
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        self._py_cursor.executemany(query, params_list)
        self._update_description()
        self._rownumber = 0

    def fetchone(self) -> Optional[Sequence[Optional[Any]]]:
        """
        Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.
        An Error (or subclass) exception is raised if the previous call to .execute() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        if self._description is None:
            raise InterfaceError('No query has been executed')

        if self._is_non_sql_query():
            raise InterfaceError('Query does not return any rows')

        res = self._py_cursor.fetchone()
        if res is None:
            self._rownumber = None
        else:
            self._rownumber += 1

        return res

    def fetchmany(self, size: Optional[int] = None) -> Optional[Sequence[Sequence[Optional[Any]]]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences. An empty sequence is returned
        when no more rows are available.

        The parameter specifies the number of rows to fetch per call. If it is not given, the cursor’s arraysize
        determines the number of rows to be fetched. The method tries to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number of rows not being available, fewer rows will be
        returned.

        An Error (or subclass) exception is raised if the previous call to .execute() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        if size is None:
            size = self.arraysize

        if size <= 0:
            raise InterfaceError(f'Size parameter should be positive [size={size}]')

        res = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            res.append(row)

        return res

    def fetchall(self) -> Optional[Sequence[Sequence[Optional[Any]]]]:
        """
        Fetch all remaining rows of a query result, returning them as a sequence of sequences.
        An Error (or subclass) exception is raised if the previous call to .execute() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        res = []
        row = self.fetchone()
        while row is not None:
            res.append(row)
            row = self.fetchone()

        return res

    def nextset(self):
        if self._py_cursor is None:
            raise InterfaceError('Cursor is already closed')

        # TODO: IGNITE-22743 Implement execution of SQL scripts
        raise NotSupportedError('Operation is not supported')

    def setinputsizes(self, *_args):
        """
        This operation does nothing currently.
        """
        pass

    def setoutputsize(self, *_args):
        """
        This operation does nothing currently.
        """
        pass


class Connection:
    """
    Connection class. Represents a single connection to an Ignite cluster.
    """

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    NotSupportedError = NotSupportedError

    def __init__(self):
        self._py_connection = None
        self._cursors = {}
        self._cursor_cnt = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close active connection.
        Closing a connection without committing the changes first will cause an implicit rollback to be performed.
        Completes without errors on successfully closed connections.
        """
        if self._py_connection is not None:
            self._py_connection.close()
            self._py_connection = None

            cursors = self._cursors
            self._cursors = {}
            for cursor in cursors.values():
                cursor.close()

    def commit(self):
        """
        Commit any pending transaction to the database.
        """
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')

        self._py_connection.commit()

    def rollback(self):
        """
        Roll back to the start of any pending transaction.
        Closing a connection without committing the changes first will cause an implicit rollback to be performed.
        """
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')

        self._py_connection.rollback()

    @property
    def autocommit(self) -> bool:
        """
        Attribute that is used to query and set the autocommit mode of the connection.
        Returns True if the connection is operating in autocommit (non-transactional) mode.
        Returns False if the connection is operating in manual commit (transactional) mode.

        Setting the attribute to True or False adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will cause the connection to leave autocommit
        mode and start a new transaction.

        Changing from False to True (enabling autocommit) will put the connection in a state, in which every query is
        executed as a separate transaction.
        """
        if self._py_connection is None:
            return True
        return self._py_connection.autocommit()

    @autocommit.setter
    def autocommit(self, value: bool):
        """
        Attribute that is used to query and set the autocommit mode of the connection.
        Setting the attribute to True or False adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will cause the connection to leave autocommit
        mode and start a new transaction.

        Changing from False to True (enabling autocommit) will put the connection in a state, in which every query is
        executed as a separate transaction.
        """
        self.setautocommit(value)

    def setautocommit(self, value: bool):
        """
        Set the autocommit mode of the connection. Adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will have the database leave autocommit mode
        and start a new transaction.

        Changing from False to True (enabling autocommit) has database-dependent semantics with respect to how pending
        transactions are handled.
        """
        if self._py_connection is not None:
            self._py_connection.set_autocommit(value)

    def cursor(self) -> Cursor:
        """
        Return a new Cursor Object using the connection.
        """
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')

        cursor_id = self._cursor_cnt
        self._cursor_cnt += 1

        cursor = Cursor(cur_id=cursor_id, py_cursor=self._py_connection.cursor(), conn=self)
        self._cursors[cursor_id] = cursor

        return cursor

    def _cursor_closed(self, cur_id: int):
        self._cursors.pop(cur_id, None)


def connect(address: [str], **kwargs) -> Connection:
    """
    Establish connection with the Ignite cluster.

    Parameters
    ----------
    address: [str]
        A list of cluster node addresses for the client to choose from. Used for initial connection and fail-over.

    Keyword Arguments
    -----------------
    identity: str, optional
        An identifier to use for authentication. E.g. username.
    secret: str, optional
        A secret to use for authentication. E.g. password.
    schema: str, optional
        A schema name to be used by default. Default value: 'PUBLIC'.
    timezone: str, optional
        A timezone to use as a client's timezone. Required to correctly work with date/time values, received from
        the client. By default, a server's timezone is used.
    page_size: int, optional
        Maximum number of rows that can be received or sent in a single request. Default value: 1024.
    timeout: int, optional
        A timeout for network operations, in seconds. Default value: 30.
    heartbeat_interval: float, optional
        An interval between heartbeat probes, in seconds. Zero or negative means heartbeats disabled. Default value: 30.
    autocommit: bool, optional
        Connection autocommit mode. Default value: True (enabled).
    use_ssl: bool, optional
        Set to True if you like to use SSL for your connection. Make sure it's configured on the server side as well.
    ssl_keyfile: str, optional
        A path to SSL key file to identify the local (client) party.
    ssl_certfile: str, optional
        A path to ssl certificate file to identify the local (client) party.
    ssl_ca_certfile: str, optional
        A path to a trusted certificate or a certificate chain.
        Required to check the validity of the remote (server-side) certificate.
    """
    return _pyignite_dbapi_extension.connect(address=address, **kwargs)

