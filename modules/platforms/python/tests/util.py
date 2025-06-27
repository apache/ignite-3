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

import os

import psutil
import signal
import subprocess
import time

import pytest

import pyignite_dbapi

server_host = os.getenv("IGNITE_CLUSTER_HOST", '127.0.0.1')
server_addresses_invalid = [server_host + ':10000']
server_addresses_basic = [server_host + ':10942', server_host + ':10943']
server_addresses_ssl_basic = [server_host + ':10944']
server_addresses_ssl_client_auth = [server_host + ':10945']
server_addresses_all = server_addresses_basic + server_addresses_ssl_basic + server_addresses_ssl_client_auth


def wait_for_condition(condition, interval=0.1, timeout=10, error=None):
    start = time.time()
    res = condition()

    while not res and time.time() - start < timeout:
        time.sleep(interval)
        res = condition()

    if res:
        return True

    if error is not None:
        raise Exception(error)

    return False


def is_windows():
    return os.name == "nt"


def get_test_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_proj_dir():
    return os.path.abspath(os.path.join(get_test_dir(), "..", "..", "..", ".."))


def get_ignite_dirs():
    ignite_home = os.getenv("IGNITE_HOME")
    if ignite_home is not None:
        yield ignite_home

    yield get_proj_dir()


def get_ignite_runner():
    ext = ".bat" if is_windows() else ""
    for ignite_dir in get_ignite_dirs():
        runner = os.path.join(ignite_dir, "gradlew" + ext)
        print("Probing Ignite runner at '{0}'...".format(runner))
        if os.path.exists(runner):
            return runner

    raise Exception("Ignite not found. Please make sure your IGNITE_HOME environment variable points to directory with "
                    "a valid Ignite instance")


def kill_process_tree(pid):
    if is_windows():
        subprocess.call(['taskkill', '/F', '/T', '/PID', str(pid)])
    else:
        children = psutil.Process(pid).children(recursive=True)
        for child in children:
            os.kill(child.pid, signal.SIGKILL)
        os.kill(pid, signal.SIGKILL)


# noinspection PyBroadException
def check_server_started(addr: str) -> bool:
    try:
        conn = pyignite_dbapi.connect(address=[addr], timeout=1)
    except pyignite_dbapi.Error:
        return False

    conn.close()
    return True


def check_cluster_started() -> bool:
    for addr in server_addresses_basic:
        if not check_server_started(addr):
            return False
    return True


def start_cluster(debug=False, jvm_opts='') -> subprocess.Popen:
    runner = get_ignite_runner()

    env = os.environ.copy()

    env['JVM_OPTS'] = env.get('JVM_OPTS', '') + jvm_opts

    if debug:
        env['JVM_OPTS'] = env.get('JVM_OPTS', '') + \
                          '-Djava.net.preferIPv4Stack=true -Xdebug -Xnoagent -Djava.compiler=NONE ' \
                          '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 '

    ignite_cmd = [runner, ':ignite-runner:runnerPlatformTest', '--no-daemon', '-x', 'compileJava', '-x',
                  'compileTestFixturesJava', '-x', 'compileIntegrationTestJava', '-x',  'compileTestJava']

    print('Starting Ignite runner:', ignite_cmd)

    ignite_dir = next(get_ignite_dirs())
    if ignite_dir is None:
        raise Exception('Can not resolve an Ignite project directory')

    cluster = subprocess.Popen(ignite_cmd, env=env, cwd=ignite_dir)

    for addr in server_addresses_basic:
        started = wait_for_condition(lambda: check_server_started(addr), timeout=180)
        if not started:
            kill_process_tree(cluster.pid)
            raise Exception('Failed to start Ignite Cluster: timeout while trying to connect')

    return cluster


def start_cluster_gen(debug=False):
    srv = start_cluster(debug=debug)
    try:
        yield srv
    finally:
        kill_process_tree(srv.pid)


def get_row(index):
    return index, f'Value-{index * 2}', index / 2.0


def row_generator(begin, rows_num):
    for i in range(begin, begin + rows_num):
        yield get_row(i)


def create_and_populate_test_table(cursor, rows_num, table_name, batch_size=1):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, data varchar, fl double)')
    if batch_size == 1:
        for i in range(rows_num):
            cursor.execute(f"insert into {table_name} values (?, ?, ?)", params=get_row(i))
    else:
        batch = 0
        for batch in range(rows_num // batch_size):
            cursor.executemany(f"insert into {table_name} values(?, ?, ?)",
                               list(row_generator(batch * batch_size, batch_size)))
        if rows_num % batch_size:
            cursor.executemany(f"insert into {table_name} values(?, ?, ?)",
                               list(row_generator(batch * batch_size, rows_num % batch_size)))


def check_row(i, row):
    assert len(row) == 3
    assert row[0] == i
    assert row[1] == f'Value-{i * 2}'
    assert row[2] == pytest.approx(i / 2.0)
