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
#include <ignite/odbc/sql_statement.h>

#include <ignite/common/detail/config.h>

#include "module.h"
#include "py_connection.h"
#include "py_cursor.h"

#include <Python.h>

int py_connection_init(py_connection *self, PyObject *args, PyObject *kwds)
{
    UNUSED_VALUE args;
    UNUSED_VALUE kwds;

    self->m_environment = nullptr;
    self->m_connection = nullptr;

    return 0;
}

void py_connection_dealloc(py_connection *self)
{
    delete self->m_connection;
    delete self->m_environment;

    self->m_connection = nullptr;
    self->m_environment = nullptr;

    Py_TYPE(self)->tp_free(self);
}

static PyObject* py_connection_close(py_connection* self, PyObject*)
{
    if (self->m_connection) {
        self->m_connection->release();
        if (!check_errors(*self->m_connection))
            return nullptr;

        delete self->m_connection;
        self->m_connection = nullptr;

        delete self->m_environment;
        self->m_environment = nullptr;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* py_connection_cursor(py_connection* self, PyObject*)
{
    if (self->m_connection) {
        std::unique_ptr<ignite::sql_statement> statement{self->m_connection->create_statement()};
        if (!check_errors(*self->m_connection))
            return nullptr;

        auto py_cursor = make_py_cursor(std::move(statement));
        if (!py_cursor)
            return nullptr;

        auto py_cursor_obj = (PyObject*)py_cursor;
        Py_INCREF(py_cursor_obj);
        return py_cursor_obj;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyTypeObject py_connection_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    MODULE_NAME "." PY_CONNECTION_CLASS_NAME
};

static struct PyMethodDef py_connection_methods[] = {
    {"close", (PyCFunction)py_connection_close, METH_NOARGS, nullptr},
    {"cursor", (PyCFunction)py_connection_cursor, METH_NOARGS, nullptr},
    {nullptr, nullptr, 0, nullptr}
};

int prepare_py_connection_type() {
    py_connection_type.tp_new = PyType_GenericNew;
    py_connection_type.tp_basicsize=sizeof(py_connection);
    py_connection_type.tp_dealloc=(destructor)py_connection_dealloc;
    py_connection_type.tp_flags=Py_TPFLAGS_DEFAULT;
    py_connection_type.tp_methods=py_connection_methods;
    py_connection_type.tp_init=(initproc)py_connection_init;

    return PyType_Ready(&py_connection_type);
}

int register_py_connection_type(PyObject* mod) {
    return PyModule_AddObjectRef(mod, PY_CONNECTION_CLASS_NAME, (PyObject *)&py_connection_type);
}

py_connection *make_py_connection(std::unique_ptr<ignite::sql_environment> env,
    std::unique_ptr<ignite::sql_connection> conn) {
    py_connection* py_conn_obj  = PyObject_New(py_connection, &py_connection_type);

    if (!py_conn_obj)
        return nullptr;

    py_conn_obj->m_environment = env.release();
    py_conn_obj->m_connection = conn.release();

    return py_conn_obj;
}
