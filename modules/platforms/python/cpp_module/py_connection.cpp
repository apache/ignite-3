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

#include "py_connection.h"

#include <ignite/odbc/sql_environment.h>
#include <ignite/odbc/sql_connection.h>

#include <Python.h>

#define MODULE_NAME "_pyignite3_extension.connection"
#define CLASS_NAME "PyConnection"

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    nullptr,                /* m_doc */
    -1,                     /* m_size */
    nullptr,                /* m_methods */
    nullptr,                /* m_slots */
    nullptr,                /* m_traverse */
    nullptr,                /* m_clear */
    nullptr,                /* m_free */
};

static int py_connection_init(py_connection *self, PyObject *args, PyObject *kwds)
{
    UNUSED_VALUE args;
    UNUSED_VALUE kwds;

    self->m_env = nullptr;
    self->m_conn = nullptr;

    return 0;
}

static void py_connection_dealloc(py_connection *self)
{
    delete self->m_conn;
    delete self->m_env;
    Py_TYPE(self)->tp_free(self);
}

PyMODINIT_FUNC PyInit__pyignite3_extension_connection(void) { // NOLINT(*-reserved-identifier)
    PyTypeObject py_connection_type = {
        PyVarObject_HEAD_INIT(nullptr, 0)
        MODULE_NAME "." CLASS_NAME
    };

    PyObject* m;

    py_connection_type.tp_new = PyType_GenericNew;
    py_connection_type.tp_basicsize=sizeof(py_connection);
    py_connection_type.tp_dealloc=(destructor)py_connection_dealloc;
    py_connection_type.tp_flags=Py_TPFLAGS_DEFAULT;
    py_connection_type.tp_init=(initproc)py_connection_init;

    if (PyType_Ready(&py_connection_type) < 0)
        return nullptr;

    m = PyModule_Create(&module_def);
    if (m == nullptr)
        return nullptr;

    Py_INCREF(&py_connection_type);
    PyModule_AddObject(m, CLASS_NAME, (PyObject *)&py_connection_type);
    return m;
}

py_connection *make_py_connection(std::unique_ptr<ignite::sql_environment> env,
    std::unique_ptr<ignite::sql_connection> conn) {
    auto local_mod = PyImport_ImportModule(MODULE_NAME);

    if (!local_mod)
        return nullptr;

    auto conn_class = PyObject_GetAttrString(local_mod, CLASS_NAME);
    Py_DECREF(local_mod);

    if (!conn_class)
        return nullptr;

    auto args = PyTuple_New(0);
    auto kwargs = Py_BuildValue("{}");
    PyObject* py_conn_obj  = PyObject_Call(conn_class, args, kwargs);
    Py_DECREF(conn_class);
    Py_DECREF(args);
    Py_DECREF(kwargs);

    if (!py_conn_obj)
        return nullptr;

    auto typed_conn = reinterpret_cast<py_connection*>(py_conn_obj);
    typed_conn->m_env = env.release();
    typed_conn->m_conn = conn.release();

    return typed_conn;
}
