/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/odbc/sql_environment.h>
#include <ignite/odbc/sql_connection.h>

#include <Python.h>

/**
 * Connection Python object.
 */
struct py_connection {
    PyObject_HEAD

    /** Environment. */
    ignite::sql_environment *m_env;

    /** Connection. */
    ignite::sql_connection *m_conn;
};

/**
 * Create a new instance of py_connection python class.
 * @param env Environment.
 * @param conn Connection.
 * @return A new class instance.
 */
py_connection* make_py_connection(std::unique_ptr<ignite::sql_environment> env,
    std::unique_ptr<ignite::sql_connection> conn);

