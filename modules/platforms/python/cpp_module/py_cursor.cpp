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

#include <ignite/odbc/sql_statement.h>

#include <ignite/common/detail/config.h>

#include "module.h"
#include "py_cursor.h"

#include <Python.h>

int py_cursor_init(py_cursor *self, PyObject *args, PyObject *kwds)
{
    UNUSED_VALUE args;
    UNUSED_VALUE kwds;

    self->m_statement = nullptr;

    return 0;
}

void py_cursor_dealloc(py_cursor *self)
{
    delete self->m_statement;
    self->m_statement = nullptr;
    Py_TYPE(self)->tp_free(self);
}

static PyObject* py_cursor_close(py_cursor* self, PyObject*)
{
    if (self->m_statement) {
        self->m_statement->close();
        if (!check_errors(*self->m_statement))
            return nullptr;

        delete self->m_statement;
        self->m_statement = nullptr;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* py_cursor_execute(py_cursor* self, PyObject* args, PyObject* kwargs)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    static char *kwlist[] = {
            "query",
            "params",
            nullptr
    };

    const char* query = nullptr;
    PyObject *params = nullptr;

    int parsed = PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", kwlist, &query, &params);

    if (!parsed)
        return nullptr;

    self->m_statement->execute_sql_query(query);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyTypeObject py_cursor_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    MODULE_NAME "." PY_CURSOR_CLASS_NAME
};

static struct PyMethodDef py_cursor_methods[] = {
    {"close", (PyCFunction)py_cursor_close, METH_NOARGS, nullptr},
    {"execute", (PyCFunction)py_cursor_execute, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr}
};

int prepare_py_cursor_type() {
    py_cursor_type.tp_new = PyType_GenericNew;
    py_cursor_type.tp_basicsize=sizeof(py_cursor);
    py_cursor_type.tp_dealloc=(destructor)py_cursor_dealloc;
    py_cursor_type.tp_flags=Py_TPFLAGS_DEFAULT;
    py_cursor_type.tp_methods=py_cursor_methods;
    py_cursor_type.tp_init=(initproc)py_cursor_init;

    return PyType_Ready(&py_cursor_type);
}

int register_py_cursor_type(PyObject* mod) {
    return PyModule_AddObjectRef(mod, PY_CURSOR_CLASS_NAME, (PyObject *)&py_cursor_type);
}

py_cursor *make_py_cursor(std::unique_ptr<ignite::sql_statement> stmt) {
    py_cursor* py_cursor_obj  = PyObject_New(py_cursor, &py_cursor_type);
    if (!py_cursor_obj)
        return nullptr;

    py_cursor_obj->m_statement = stmt.release();

    return py_cursor_obj;
}
