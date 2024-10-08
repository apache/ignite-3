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
from typing import Optional, List, Any, Sequence, Tuple, Union

from pyignite3 import _pyignite3_extension
from pyignite3 import native_type_code

__version__ = '3.0.0-beta2'

# PEP 249 is supported
apilevel = '2.0'

# Threads may share the module, but not connections.
threadsafety = 1

# Parameter style is a question mark, e.g. '...WHERE name=?'
paramstyle = 'qmark'

# Null constant
NULL = None

# Boolean type
BOOLEAN = bool

# Integer type
INT = int

# Floating point type
FLOAT = float

# String type
STRING = str

# Binary type
BINARY = bytes

# Big number (Decimal) type
NUMBER = decimal.Decimal

# Date type
DATE = datetime.date

# Time type
TIME = datetime.time

# Date-Time type
DATETIME = datetime.datetime

# Duration type
DURATION = datetime.timedelta

# UUID type
UUID = uuid.UUID

# This type object is used to describe the “Row ID” column in a database.
ROWID = UUID


class TIMESTAMP(float):
    """
    Timestamp data type.
    """
    pass


# noinspection PyPep8Naming
def Date(year, month, day):
    """
    This function constructs an object holding a date value.
    """
    return DATE(year=year, month=month, day=day)


# noinspection PyPep8Naming
def Time(hour, minute, second):
    """
    This function constructs an object holding a time value.
    """
    return TIME(hour=hour, minute=minute, second=second)


# noinspection PyPep8Naming
def Timestamp(year, month, day, hour, minute, second):
    """
    This function constructs an object holding a time stamp value.
    """
    dt = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
    ts = dt.timestamp()
    return TIMESTAMP(ts)


# noinspection PyPep8Naming
def DateFromTicks(ticks):
    """
    This function constructs an object holding a date value from the given ticks value (number of seconds since
    the epoch; see the documentation of the standard Python time module for details).
    """
    return DATE.fromtimestamp(ticks)


# noinspection PyPep8Naming
def TimeFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given ticks value (number of seconds since
    the epoch; see the documentation of the standard Python time module for details).
    """
    return DATETIME.fromtimestamp(ticks).time()


# noinspection PyPep8Naming
def TimestampFromTicks(ticks):
    """
    This function constructs an object holding a time stamp value from the given ticks value (number of seconds since
    the epoch; see the documentation of the standard Python time module for details).
    """
    return TIMESTAMP(ticks)


# noinspection PyPep8Naming
def Binary(string: str):
    """
    This function constructs an object capable of holding a binary (long) string value.
    """
    return BINARY(string, 'utf-8')


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


class ColumnDescription:
    """
    Represents a description of the single column of the result set.
    """

    def __init__(self, name: str, type_code: int, display_size: Optional[int], internal_size: Optional[int],
                 precision: Optional[int], scale: Optional[int], null_ok: bool):
        self.name = name
        self.type_code = _type_code_from_int(type_code)
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.null_ok = null_ok


class Cursor:
    """
    Cursor class. Represents a single statement and holds the result of its execution.

    Attributes
    ----------
    arraysize: int
        a read/write attribute, that specifies the number of rows to fetch at a time with .fetchmany().
        It defaults to 1 meaning to fetch a single row at a time.
    """
    arraysize: int = 1

    def __init__(self, py_cursor, conn):
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
        Return the next row from the currently executing SQL statement using the same semantics as .fetchone().
        A StopIteration exception is raised when the result set is exhausted.
        """
        res = self.fetchone()
        if res is None:
            raise StopIteration
        return res

    @property
    def description(self) -> Optional[List[ColumnDescription]]:
        """
        This read-only attribute is a sequence of 7-item sequences.
        Each of these sequences contains information describing one result column:
        - name
        - type_code
        - display_size
        - internal_size
        - precision
        - scale
        - null_ok
        The first two items (name and type_code) are mandatory, the other five are optional and are set to None if
        no meaningful values can be provided.
        This attribute will be None for operations that do not return rows or if the cursor has not had an operation
        invoked via the .execute*() method yet.
        """
        if self._py_cursor is None:
            return None
        return self._description

    @property
    def rowcount(self) -> int:
        """
        This read-only attribute specifies the number of rows that the last .execute*() produced (for DQL statements
        like SELECT) or affected (for DML statements like UPDATE or INSERT).
        The attribute is -1 in case no .execute*() has been performed on the cursor or the rowcount of the last
        operation is cannot be determined by the interface.
        """
        if self._py_cursor is None:
            return -1
        return self._py_cursor.rowcount()

    @property
    def rownumber(self) -> Optional[int]:
        """
        This read-only attribute provides the current 0-based index of the cursor in the result set or None if the index
        cannot be determined.
        The index can be seen as index of the cursor in a sequence (the result set). The next fetch operation will fetch
        the row indexed by .rownumber in that sequence.
        """
        return self._rownumber

    @property
    def connection(self):
        """
        This read-only attribute return a reference to the Connection object on which the cursor was created.
        """
        return self._conn

    @property
    def lastrowid(self):
        """
        This read-only attribute provides the rowid of the last modified row (most databases return a rowid only when a
        single INSERT operation is performed). As Ignite does not support rowids, this attribute is always set to None.
        """
        return None

    def callproc(self, *_args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        raise NotSupportedError('Stored procedures are not supported')

    def close(self):
        """
        Close active cursor.
        Completes without errors on successfully closed cursors.
        """
        if self._py_cursor is not None:
            self._py_cursor.close()
            self._py_cursor = None
            self._rownumber = None

    def execute(self, query: str, params: Optional[Union[List[Any], Tuple[Any]]] = None):
        """
        Execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
        Arguments are specified as a question mark '?' in the request.

        The parameters may also be specified as list of tuples to e.g. insert multiple rows in a single operation,
        but this kind of usage is deprecated: .executemany() should be used instead.
        """
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        self._py_cursor.execute(query, params)
        self._update_description()
        self._rownumber = 0

    def _update_description(self):
        """
        Internal method.
        Update column description for the current cursor. To be called after query execution.
        """
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

    def executemany(self, *_args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22742 Implement execution with a batch of parameters
        raise NotSupportedError('Operation is not supported')

    def fetchone(self) -> Optional[Sequence[Optional[Any]]]:
        """
        Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.
        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

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

        The number of rows to fetch per call is specified by the parameter. If it is not given, the cursor’s arraysize
        determines the number of rows to be fetched. The method tries to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number of rows not being available, fewer rows will be
        returned.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

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

        return None if not res else res

    def fetchall(self) -> Optional[Sequence[Sequence[Optional[Any]]]]:
        """
        Fetch all remaining rows of a query result, returning them as a sequence of sequences.
        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set
        or no call was issued yet.
        """
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        res = []
        row = self.fetchone()
        while row is not None:
            res.append(row)
            row = self.fetchone()

        return None if not res else res

    def nextset(self):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22743 Implement execution of SQL scripts
        raise NotSupportedError('Operation is not supported')

    def setinputsizes(self, *_args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22742 Implement execution with a batch of parameters
        raise NotSupportedError('Operation is not supported')

    def setoutputsize(self, *args):
        """
        This operation does nothing currently.
        """
        pass


class Connection:
    """
    Connection class. Represents a single connection to the Ignite cluster.
    """

    def __init__(self):
        self._autocommit = True
        self._py_connection = None

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
        Attribute to query and set the autocommit mode of the connection.
        Return True if the connection is operating in autocommit (non-transactional) mode. Return False if
        the connection is operating in manual commit (transactional) mode.

        Setting the attribute to True or False adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will have the database leave autocommit mode
        and start a new transaction.

        Changing from False to True (enabling autocommit) has database dependent semantics with respect to how pending
        transactions are handled.
        """
        if self._py_connection is None:
            return True
        return self._py_connection.autocommit()

    @autocommit.setter
    def autocommit(self, value):
        """
        Attribute to query and set the autocommit mode of the connection.
        Setting the attribute to True or False adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will have the database leave autocommit mode
        and start a new transaction.

        Changing from False to True (enabling autocommit) has database dependent semantics with respect to how pending
        transactions are handled.
        """
        self.setautocommit(value)

    def setautocommit(self, value: bool):
        """
        Set the autocommit mode of the connection. Adjusts the connection’s mode accordingly.

        Changing the setting from True to False (disabling autocommit) will have the database leave autocommit mode
        and start a new transaction.

        Changing from False to True (enabling autocommit) has database dependent semantics with respect to how pending
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
        return Cursor(py_cursor=self._py_connection.cursor(), conn=self)


def connect(address: [str], **kwargs) -> Connection:
    """
    Establish connection with the Ignite cluster.

    Parameters
    ----------
    address: [str]
        A list of addresses of cluster nodes for client to choose from. Used for initial connection and fail-over.

    Keyword Arguments
    ----------
    identity: str, optional
        An identifier to use for authentication. E.g. username.
    secret: str, optional
        A secret to use for authentication. E.g. password.
    schema: str, optional
        A schema name to be used by default. Default value: 'PUBLIC'.
    timezone: str, optional
        A timezone to use as a client's timezone. Used to correctly work with date/time values, received from client.
        By default, a server's timezone is used.
    page_size: int, optional
        A maximum number of rows, which are received or sent in a single request. Default value: 1024.
    timeout: int, optional
        A timeout in seconds to use for any network operation. Default value: 30.
    autocommit: bool, optional
        The autocommit mode of the connection. Default value: True.
    """
    return _pyignite3_extension.connect(address=address, **kwargs)


class Error(Exception):
    """
    Exception that is the base class of all other error exceptions. You can use this to catch all errors with one single
    except statement. Warnings are not considered errors and thus should not use this class as base.
    """
    pass


# noinspection PyShadowingBuiltins
class Warning(Exception):
    """
    Exception raised for important warnings like data truncations while inserting, etc.
    """
    pass


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface rather than the database itself.
    """
    pass


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """
    pass


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the processed data like division by zero, numeric value
    out of range, etc..
    """
    pass


class InternalError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected, e.g. a foreign key check fails.
    """
    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database’s operation and not necessarily under the control of
    the programmer, e.g. an unexpected disconnect occurs, the data source name is not found, a transaction could not be
    processed, a memory allocation error occurred during processing, etc.
    """
    pass


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors, e.g. table not found or already exists, syntax error in the SQL statement,
    wrong number of parameters specified, etc.
    """
    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected, e.g. a foreign key check fails.
    """
    pass


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is not supported by the database.
    """
    pass
