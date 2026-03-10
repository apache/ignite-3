.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License. You may obtain a copy of the License at

..     http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

PyIgnite DB API documentation
=============================

This is a documentation for Apache Ignite 3 DB API Driver. It implemented to provide implementation of `PEP-0249`_ for
Apache Ignite 3.

.. _PEP-0249: https://peps.python.org/pep-0249

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Connection Constructor
====================================

This is the interface entry point.

.. autofunction:: pyignite_dbapi.connect


Connection Object
====================================

.. autoclass:: pyignite_dbapi.Connection
   :members:


Cursor Object
=============

.. autoclass:: pyignite_dbapi.Cursor
   :members:


ColumnDescription Object
========================

.. autoclass:: pyignite_dbapi.ColumnDescription


Globals
=======

Module defines following globals:

.. autodata:: pyignite_dbapi.apilevel
.. autodata:: pyignite_dbapi.threadsafety
.. autodata:: pyignite_dbapi.paramstyle


Type Objects and Constructors
=============================

The module exports the following constructors and singletons:

.. autodata:: pyignite_dbapi.NULL
.. autodata:: pyignite_dbapi.BOOLEAN
.. autodata:: pyignite_dbapi.INT
.. autodata:: pyignite_dbapi.FLOAT
.. autodata:: pyignite_dbapi.STRING
.. autodata:: pyignite_dbapi.BINARY
.. autodata:: pyignite_dbapi.NUMBER
.. autodata:: pyignite_dbapi.DATE
.. autodata:: pyignite_dbapi.TIME
.. autodata:: pyignite_dbapi.DATETIME
.. autodata:: pyignite_dbapi.DURATION
.. autodata:: pyignite_dbapi.UUID
.. autodata:: pyignite_dbapi.ROWID
.. autodata:: pyignite_dbapi.TIMESTAMP
.. autofunction:: pyignite_dbapi.Date
.. autofunction:: pyignite_dbapi.Time
.. autofunction:: pyignite_dbapi.Timestamp
.. autofunction:: pyignite_dbapi.DateFromTicks
.. autofunction:: pyignite_dbapi.TimeFromTicks
.. autofunction:: pyignite_dbapi.TimestampFromTicks
.. autofunction:: pyignite_dbapi.Binary


Exceptions
==========

The module defines the following error classes:

.. autoexception:: pyignite_dbapi.Error
.. autoexception:: pyignite_dbapi.Warning
.. autoexception:: pyignite_dbapi.InterfaceError
.. autoexception:: pyignite_dbapi.DatabaseError
.. autoexception:: pyignite_dbapi.DataError
.. autoexception:: pyignite_dbapi.InternalError
.. autoexception:: pyignite_dbapi.OperationalError
.. autoexception:: pyignite_dbapi.ProgrammingError
.. autoexception:: pyignite_dbapi.IntegrityError
.. autoexception:: pyignite_dbapi.NotSupportedError
