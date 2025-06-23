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

#pragma once

#include <string>

#include <Python.h>

namespace ignite {
class sql_environment;
class sql_connection;
}

/**
 * Connection Python object.
 */
struct py_connection {
    PyObject_HEAD

    /** Environment. */
    ignite::sql_environment *m_environment;

    /** Connection. */
    ignite::sql_connection *m_connection;
};

/**
 * Connection init function.
 */
int py_connection_init(py_connection *self, PyObject *args, PyObject *kwds);

/**
 * Connection dealloc function.
 */
void py_connection_dealloc(py_connection *self);

/**
 * Create a new instance of py_connection python class.
 *
 * @param address_str Address string.
 * @param schema Schema.
 * @param identity Identity.
 * @param secret Secret.
 * @param page_size Page size.
 * @param timeout Timeout.
 * @param autocommit Autocommit flag.
 * @return A new connection class instance.
 */
py_connection* make_py_connection(std::string address_str, const char* schema, const char* identity, const char* secret,
    int page_size, int timeout, int autocommit);

/**
 * Prepare PyConnection type for registration.
 */
int prepare_py_connection_type();

/**
 * Register PyConnection type within module.
 */
int register_py_connection_type(PyObject* mod);