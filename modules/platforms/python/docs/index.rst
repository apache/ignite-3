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

PyIgnite3 documentation
=======================

This is a documentation for Apache Ignite 3 DB API Driver. It implemented to provide implementation of `PEP-0249`_ for
Apache Ignite 3.

.. _PEP-0249: https://peps.python.org/pep-0249

.. toctree::
   :maxdepth: 2
   :caption: Contents:


Module Interface
================

In this section you can find a module-level functions and types.

Connection Constructor
----------------------

This is the interface entry point.

.. autofunction:: pyignite3.connect

Type Objects and Constructors
-----------------------------

The module exports the following constructors and singletons:

.. autodata:: pyignite3.apilevel
.. autodata:: pyignite3.threadsafety
.. autodata:: pyignite3.paramstyle
.. autodata:: pyignite3.NULL
.. autodata:: pyignite3.BOOLEAN
.. autodata:: pyignite3.INT
.. autodata:: pyignite3.FLOAT
.. autodata:: pyignite3.STRING
.. autodata:: pyignite3.BINARY
.. autodata:: pyignite3.NUMBER
.. autodata:: pyignite3.DATE
.. autodata:: pyignite3.TIME
.. autodata:: pyignite3.DATETIME
.. autodata:: pyignite3.DURATION
.. autodata:: pyignite3.UUID
.. autodata:: pyignite3.ROWID
.. autodata:: pyignite3.TIMESTAMP
.. autofunction:: pyignite3.Date
.. autofunction:: pyignite3.Time
.. autofunction:: pyignite3.Timestamp
.. autofunction:: pyignite3.DateFromTicks
.. autofunction:: pyignite3.TimeFromTicks
.. autofunction:: pyignite3.TimestampFromTicks
.. autofunction:: pyignite3.Binary
