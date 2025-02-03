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

#include "module.h"
#include "utils.h"
#include "py_connection.h"
#include "py_cursor.h"

#include <Python.h>

/**
 * Check if the connection is open. Set error if not.
 *
 * @param self Connection.
 * @return @c true if open and @c false otherwise.
 */
static bool py_connection_expect_open(py_connection* self) {
    if (!self->m_connection) {
        PyErr_SetString(py_get_module_interface_error_class(), "Connection is in invalid state (Already closed?)");
        return false;
    }
    return true;
}

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

    Py_RETURN_NONE;
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

    Py_RETURN_NONE;
}

static PyObject* py_connection_autocommit(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    SQLUINTEGER res = 0;
    self->m_connection->get_attribute(SQL_ATTR_AUTOCOMMIT, &res, 0, nullptr);
    if (!check_errors(*self->m_connection))
        return nullptr;

    if (!res) {
        Py_RETURN_FALSE;
    }

    Py_RETURN_TRUE;
}

static PyObject* py_connection_set_autocommit(py_connection* self, PyObject* value)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    if (!PyBool_Check(value)) {
        PyErr_SetString(py_get_module_interface_error_class(), "Autocommit attribute should be of a type bool");
        return nullptr;
    }

    void* ptr_autocommit = (void*)(ptrdiff_t((value == Py_True) ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF));
    self->m_connection->set_attribute(SQL_ATTR_AUTOCOMMIT, ptr_autocommit, 0);
    if (!check_errors(*self->m_connection))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_connection_commit(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    self->m_connection->transaction_commit();
    if (!check_errors(*self->m_connection))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_connection_rollback(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    self->m_connection->transaction_rollback();
    if (!check_errors(*self->m_connection))
        return nullptr;

    Py_RETURN_NONE;
}

static PyTypeObject py_connection_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    EXT_MODULE_NAME "." PY_CONNECTION_CLASS_NAME
};

static struct PyMethodDef py_connection_methods[] = {
    {"close", (PyCFunction)py_connection_close, METH_NOARGS, nullptr},
    {"cursor", (PyCFunction)py_connection_cursor, METH_NOARGS, nullptr},
    {"autocommit", (PyCFunction)py_connection_autocommit, METH_NOARGS, nullptr},
    {"set_autocommit", (PyCFunction)py_connection_set_autocommit, METH_O, nullptr},
    {"commit", (PyCFunction)py_connection_commit, METH_NOARGS, nullptr},
    {"rollback", (PyCFunction)py_connection_rollback, METH_NOARGS, nullptr},
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
    auto res = PyModule_AddObject(mod, PY_CONNECTION_CLASS_NAME, (PyObject *)&py_connection_type);
    if (res < 0) {
        Py_DECREF((PyObject *)&py_connection_type);
    }
    return res;
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
