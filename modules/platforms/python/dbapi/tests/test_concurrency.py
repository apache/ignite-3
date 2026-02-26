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

import threading
import time
from builtins import ExceptionGroup

import pytest

import pyignite_dbapi
from tests.util import server_addresses_basic


CONNECT_KWARGS = {"address": server_addresses_basic}
NUM_THREADS = 50


@pytest.fixture()
def module_level_threadsafety():
    assert pyignite_dbapi.threadsafety >= 1, "Module can not be used concurrently"


@pytest.fixture()
def connection_level_threadsafety(module_level_threadsafety):
    assert pyignite_dbapi.threadsafety >= 2, "Connections can not be used concurrently"


@pytest.fixture()
def table(table_name, service_cursor, drop_table_cleanup):
    service_cursor.execute(f"CREATE TABLE {table_name} (id int primary key, data varchar)")
    yield table_name


def run_threads(fn, n=NUM_THREADS, *args):
    barrier = threading.Barrier(n)
    errors = []

    def wrapper(tid):
        try:
            barrier.wait()
            fn(tid, *args)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=wrapper, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        raise ExceptionGroup("thread errors", errors)


def test_concurrent_module_import(module_level_threadsafety):
    import importlib

    def task(_):
        m = importlib.import_module(pyignite_dbapi.__name__)
        assert m.threadsafety > 0, "Module can not be used concurrently"

    run_threads(task)


def test_concurrent_connect_use_close(module_level_threadsafety):
    def task(_):
        c = pyignite_dbapi.connect(**CONNECT_KWARGS)
        with c.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() is not None
        c.close()

    run_threads(task)


def test_shared_connection_per_thread_cursors(connection, connection_level_threadsafety):
    def task(_):
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row is not None

    run_threads(task)


def test_concurrent_inserts_no_lost_writes(table, connection, connection_level_threadsafety):
    rows_per_thread = 50

    def task(thread_id):
        with connection.cursor() as cur:
            for i in range(rows_per_thread):
                cur.execute(f"INSERT INTO {table} (id, data) VALUES (?, ?)", (thread_id * rows_per_thread + i, f"v{thread_id}-{i}"))

    run_threads(task)

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
    assert count == NUM_THREADS * rows_per_thread


def test_concurrent_commit_and_rollback(table, connection, connection_level_threadsafety):
    """Half the threads commit, half rollback. Only committed rows appear."""
    committed_ids = []
    lock = threading.Lock()

    def task(thread_id):
        connection.autocommit = False
        with connection.cursor() as cur:
            cur.execute(f"INSERT INTO {table} (id, data) VALUES (?, ?)", (thread_id, "x"))
        if thread_id % 2 == 0:
            connection.commit()
            with lock:
                committed_ids.append(thread_id)
        else:
            connection.rollback()

    run_threads(task)

    time.sleep(5.0)

    with connection.cursor() as cur:
        cur.execute(f"SELECT id FROM {table} ORDER BY id")
        found_ids = {row[0] for row in cur.fetchall()}

    assert found_ids == set(committed_ids)


def test_concurrent_fetchall_result_integrity(table, connection, connection_level_threadsafety):
    rows_num = 200
    with connection.cursor() as cur:
        cur.executemany(f"INSERT INTO {table} (id, data) VALUES (?, ?)", [(i, f"val-{i}") for i in range(rows_num)])

    errors = []

    def task(_):
        with connection.cursor() as cur:
            cur.execute(f"SELECT id, data FROM {table} ORDER BY id")
            rows = cur.fetchall()
        if len(rows) != rows_num:
            errors.append(AssertionError(f"Expected {rows_num} rows, got {len(rows)}"))
            return
        for idx, (rid, val) in enumerate(rows):
            if val != f"val-{rid}":
                errors.append(AssertionError(f"Corrupted row: id={rid}, val={val!r}"))

    run_threads(task)
    if errors:
        raise ExceptionGroup("result integrity errors", errors)


def test_cursor_description_thread_safety(table, connection, connection_level_threadsafety):
    expected_names = {"ID", "DATA"}

    def task(_):
        with connection.cursor() as cur:
            cur.execute(f"SELECT id, data FROM {table} LIMIT 1")
            desc = cur.description
            assert desc is not None
            col_names = {col[0] for col in desc}
            assert col_names == expected_names, f"Unexpected columns: {col_names}"

    run_threads(task)


def test_concurrent_executemany(table, connection, connection_level_threadsafety):
    rows_per_thread = 20

    def task(thread_id):
        rows = [(thread_id * 1000 + i, f"{thread_id}-{i}") for i in range(rows_per_thread)]
        with connection.cursor() as cur:
            cur.executemany(f"INSERT INTO {table} (id, data) VALUES (?, ?)", rows)

    run_threads(task)

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]

    assert count == NUM_THREADS * rows_per_thread
