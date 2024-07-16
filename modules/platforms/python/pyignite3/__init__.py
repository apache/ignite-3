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

from pyignite3 import _pyignite3_extension

__version__ = '3.0.0-beta2'

# PEP 249 is supported
apilevel = '2.0'

# Threads may share the module, but not connections.
threadsafety = 1

# Parameter style is a question mark, e.g. '...WHERE name=?'
paramstyle = 'qmark'


class Cursor:
    def callproc(self, *args):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Stored procedures are not supported')

    def close(self):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def execute(self, *args):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def executemany(self, *args):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def fetchone(self):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def fetchmany(self):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def fetchall(self):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def nextset(self):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def arraysize(self) -> int:
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def setinputsizes(self, *args):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')

    def setoutputsize(self, *args):
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')


class Connection:
    """
    Connection class. Represents a single connection to the Ignite cluster.
    """
    def __init__(self):
        self._py_connection = None

    def close(self):
        """
        Close active connection.
        Completes without errors on successfully closed connections.
        """
        if self._py_connection is not None:
            self._py_connection.close()
            self._py_connection = None

    def commit(self):
        # TODO: IGNITE-22226 Implement transaction support
        raise NotSupportedError('Transactions are not supported')

    def rollback(self):
        # TODO: IGNITE-22226 Implement transaction support
        raise NotSupportedError('Transactions are not supported')

    def cursor(self) -> Cursor:
        # TODO: IGNITE-22226 Implement cursor support
        raise NotSupportedError('Operation is not supported')


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
