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
from typing import Optional

from pyignite3 import _pyignite3_extension

__version__ = '3.0.0-beta2'

# PEP 249 is supported
apilevel = '2.0'

# Threads may share the module, but not connections.
threadsafety = 1

# Parameter style is a question mark, e.g. '...WHERE name=?'
paramstyle = 'qmark'

BOOLEAN = bool
INT = int
FLOAT = float
STRING = str
BINARY = memoryview
NUMBER = decimal.Decimal
DATE = datetime.date
TIME = datetime.time
DATETIME = datetime.datetime
TIMESTAMP = datetime.datetime
UUID = uuid.UUID


class ColumnDescription:
    def __init__(self, name: str, type_code: int, display_size: Optional[int], internal_size: Optional[int], precision: Optional[int], scale: Optional[int], null_ok: bool):
        pass

class Cursor:
    """
    Cursor class. Represents a single statement and holds the result of its execution.
    """
    def __init__(self, py_cursor):
        self._py_cursor = py_cursor

        # TODO: IGNITE-22741 Implement data fetching
        self.arraysize = 1
        self._description = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def description(self):
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
        # TODO: IGNITE-22469 Implement query execution
        return self._description

    @property
    def rowcount(self) -> int:
        """
        This read-only attribute specifies the number of rows that the last .execute*() produced
        (for DQL statements like SELECT) or affected (for DML statements like UPDATE or INSERT).
        The attribute is -1 in case no .execute*() has been performed on the cursor or the rowcount of the last
        operation is cannot be determined by the interface.
        """
        if self._py_cursor is None:
            return -1
        return self._py_cursor.rowcount()

    def callproc(self, *args):
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

    def execute(self, *args):
        """
        Execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
        Arguments are specified as a question mark '?' in the request.

        The parameters may also be specified as list of tuples to e.g. insert multiple rows in a single operation,
        but this kind of usage is deprecated: .executemany() should be used instead.
        """
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        self._py_cursor.execute(*args)
        for column_id in self._py_cursor.column_count():


    def executemany(self, *args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22742 Implement execution with a batch of parameters
        raise NotSupportedError('Operation is not supported')

    def fetchone(self):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22741 Implement data fetching
        raise NotSupportedError('Operation is not supported')

    def fetchmany(self):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22741 Implement data fetching
        raise NotSupportedError('Operation is not supported')

    def fetchall(self):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22741 Implement data fetching
        raise NotSupportedError('Operation is not supported')

    def nextset(self):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22743 Implement execution of SQL scripts
        raise NotSupportedError('Operation is not supported')

    def setinputsizes(self, *args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22742 Implement execution with a batch of parameters
        raise NotSupportedError('Operation is not supported')

    def setoutputsize(self, *args):
        if self._py_cursor is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22741 Implement data fetching
        raise NotSupportedError('Operation is not supported')


class Connection:
    """
    Connection class. Represents a single connection to the Ignite cluster.
    """
    def __init__(self):
        self._py_connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close active connection.
        Completes without errors on successfully closed connections.
        """
        if self._py_connection is not None:
            self._py_connection.close()
            self._py_connection = None

    def commit(self):
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22740 Implement transaction support
        raise NotSupportedError('Transactions are not supported')

    def rollback(self):
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')

        # TODO: IGNITE-22740 Implement transaction support
        raise NotSupportedError('Transactions are not supported')

    def cursor(self) -> Cursor:
        if self._py_connection is None:
            raise InterfaceError('Connection is already closed')
        return Cursor(self._py_connection.cursor())


def connect(**kwargs) -> Connection:
    """
    Establish connection with the Ignite cluster.
    """
    return _pyignite3_extension.connect(**kwargs)


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
