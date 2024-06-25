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

import contextlib
import os

import psutil
import signal
import subprocess
import time


@contextlib.contextmanager
def get_or_create_cache(client, settings):
    cache = client.get_or_create_cache(settings)
    try:
        yield cache
    finally:
        cache.destroy()


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


def start_cluster(debug=False, jvm_opts=''):
    runner = get_ignite_runner()

    env = os.environ.copy()

    env["JVM_OPTS"] = env.get("JVM_OPTS", '') + jvm_opts

    if debug:
        env["JVM_OPTS"] = env.get("JVM_OPTS", '') + \
                          "-Djava.net.preferIPv4Stack=true -Xdebug -Xnoagent -Djava.compiler=NONE " \
                          "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 "

    ignite_cmd = [runner, " :ignite-runner:runnerPlatformTest"
                          " --no-daemon"
                          " -x compileJava"
                          " -x compileTestFixturesJava"
                          " -x compileIntegrationTestJava"
                          " -x compileTestJava"]

    print("Starting Ignite runner:", ignite_cmd)

    srv = subprocess.Popen(ignite_cmd, env=env, cwd=get_test_dir())
    time.sleep(60)
    return srv

    # TODO: Implement checking for server startup
    # started = wait_for_condition(lambda: check_server_started(idx), timeout=60)
    # if started:
    #    return srv

    # kill_process_tree(srv.pid)
    # raise Exception("Failed to start Ignite: timeout while trying to connect")


def start_cluster_gen(debug=False):
    srv = start_cluster(debug=debug)
    try:
        yield srv
    finally:
        kill_process_tree(srv.pid)
