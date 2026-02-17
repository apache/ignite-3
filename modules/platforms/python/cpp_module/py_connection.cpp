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

#include "node_connection.h"
#include "statement.h"
#include "py_connection.h"
#include "py_cursor.h"
#include "module.h"
#include "utils.h"

#include <Python.h>

#define PY_CONNECTION_CLASS_NAME "PyConnection"

namespace {

/**
 * Connection Python object.
 */
struct py_connection {
    PyObject_HEAD

    std::shared_ptr<node_connection> *m_connection;
};

/**
 * Check if the connection is open. Set error if not.
 *
 * @param self Connection.
 * @return @c true if open and @c false, otherwise.
 */
bool py_connection_expect_open(const py_connection* self) {
    if (!self->m_connection) {
        PyErr_SetString(py_get_module_interface_error_class(), "Connection is in invalid state (Already closed?)");
        return false;
    }
    return true;
}

PyObject* py_connection_close(py_connection* self, PyObject*)
{
    if (self->m_connection) {
        (*self->m_connection)->close();

        delete self->m_connection;
        self->m_connection = nullptr;
    }

    Py_RETURN_NONE;
}

PyObject* py_connection_cursor(py_connection* self, PyObject*)
{
    if (self->m_connection) {
        std::unique_ptr<statement> stmt = std::make_unique<statement>(**self->m_connection);

        auto py_cursor = make_py_cursor(std::move(stmt));
        if (!py_cursor)
            return nullptr;

        auto py_cursor_obj = reinterpret_cast<PyObject *>(py_cursor);
        Py_INCREF(py_cursor_obj);
        return py_cursor_obj;
    }

    Py_RETURN_NONE;
}

PyObject* py_connection_autocommit(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    if ((*self->m_connection)->is_auto_commit()) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

PyObject* py_connection_set_autocommit(py_connection* self, PyObject* value)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    if (!PyBool_Check(value)) {
        PyErr_SetString(py_get_module_interface_error_class(), "Autocommit attribute should be of a boolean type");
        return nullptr;
    }

    (*self->m_connection)->set_autocommit(value == Py_True);

    Py_RETURN_NONE;
}

PyObject* py_connection_commit(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    try {
        (*self->m_connection)->transaction_commit();
    } catch (const ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }
    Py_RETURN_NONE;
}

PyObject* py_connection_rollback(py_connection* self, PyObject*)
{
    if (!py_connection_expect_open(self))
        return nullptr;

    try {
        (*self->m_connection)->transaction_rollback();
    } catch (const ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }

    Py_RETURN_NONE;
}

PyTypeObject py_connection_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    EXT_MODULE_NAME "." PY_CONNECTION_CLASS_NAME
};

PyMethodDef py_connection_methods[] = {
    {"close", PyCFunction(py_connection_close), METH_NOARGS, nullptr},
    {"cursor", PyCFunction(py_connection_cursor), METH_NOARGS, nullptr},
    {"autocommit", PyCFunction(py_connection_autocommit), METH_NOARGS, nullptr},
    {"set_autocommit", PyCFunction(py_connection_set_autocommit), METH_O, nullptr},
    {"commit", PyCFunction(py_connection_commit), METH_NOARGS, nullptr},
    {"rollback", PyCFunction(py_connection_rollback), METH_NOARGS, nullptr},
    {nullptr, nullptr, 0, nullptr}
};

} // anonymous namespace

/**
 * Connection init function.
 */
int py_connection_init(py_connection *self, PyObject *, PyObject *)
{
    delete self->m_connection;
    self->m_connection = nullptr;

    return 0;
}

/**
 * Connection dealloc function.
 */
void py_connection_dealloc(py_connection *self)
{
    delete self->m_connection;
    self->m_connection = nullptr;

    Py_TYPE(self)->tp_free(self);
}

int prepare_py_connection_type() {
    py_connection_type.tp_new = PyType_GenericNew;
    py_connection_type.tp_basicsize = sizeof(py_connection);
    py_connection_type.tp_dealloc = reinterpret_cast<destructor>(py_connection_dealloc);
    py_connection_type.tp_flags = Py_TPFLAGS_DEFAULT;
    py_connection_type.tp_methods = py_connection_methods;
    py_connection_type.tp_init = reinterpret_cast<initproc>(py_connection_init);

    return PyType_Ready(&py_connection_type);
}

int register_py_connection_type(PyObject* mod) {
    auto res = PyModule_AddObject(mod, PY_CONNECTION_CLASS_NAME, reinterpret_cast<PyObject *>(&py_connection_type));
    if (res < 0) {
        Py_DECREF(reinterpret_cast<PyObject *>(&py_connection_type));
    }
    return res;
}

PyObject *make_py_connection(std::vector<ignite::end_point> addresses, const char* schema, const char* identity,
    const char* secret, int page_size, int timeout, double heartbeat_interval, bool autocommit, ssl_config &&ssl_cfg) {
    if (addresses.empty()) {
        PyErr_SetString(py_get_module_interface_error_class(), "No addresses provided to connect");
        return nullptr;
    }

    if (heartbeat_interval < 0.0)
        heartbeat_interval = 0.0;

    auto heartbeat_interval_chrono = std::chrono::milliseconds(static_cast<int>(std::ceilf(heartbeat_interval * 1000)));
    node_connection::configuration cfg{addresses, autocommit, ssl_cfg, heartbeat_interval_chrono};

    if (schema)
        cfg.m_schema = schema;

    if (identity)
        cfg.m_auth_configuration.m_identity = identity;

    if (secret)
        cfg.m_auth_configuration.m_secret = secret;

    if (page_size)
        cfg.m_page_size = page_size;

    if (timeout)
        cfg.m_timeout = timeout;

    auto node_connection = std::make_shared<class node_connection>(cfg);

    try {
        node_connection->establish();
    } catch (ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }

    py_connection* py_conn_obj = PyObject_New(py_connection, &py_connection_type);
    if (!py_conn_obj)
        return nullptr;

    py_conn_obj->m_connection = new std::shared_ptr<class node_connection>(std::move(node_connection));

    return reinterpret_cast<PyObject *>(py_conn_obj);
}
