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

#include "module.h"
#include "utils.h"
#include "py_cursor.h"
#include "statement.h"
#include "type_conversion.h"

#include <Python.h>

namespace {

/**
 * Check if the cursor is open. Set error if not.
 *
 * @param self Cursor.
 * @return @c true if open and @c false, otherwise.
 */
bool py_cursor_expect_open(const py_cursor* self) {
    if (!self->m_statement) {
        PyErr_SetString(py_get_module_interface_error_class(), "Cursor is in invalid state (Already closed?)");
        return false;
    }
    return true;
}

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

PyObject* py_cursor_close(py_cursor* self, PyObject*)
{
    if (self->m_statement) {
        self->m_statement->close();

        delete self->m_statement;
        self->m_statement = nullptr;
    }

    Py_RETURN_NONE;
}

PyObject* py_cursor_execute(py_cursor* self, PyObject* args, PyObject* kwargs)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    static char *kwlist[] = {
        const_cast<char*>("query"),
        const_cast<char*>("params"),
        nullptr
    };

    const char* query = nullptr;
    PyObject *params = nullptr;

    int parsed = PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", kwlist, &query, &params);
    if (!parsed)
        return nullptr;

    Py_ssize_t size{0};
    if (params && params != Py_None) {
        if (PySequence_Check(params)) {
            size = PySequence_Size(params);
            if (size < 0) {
                PyErr_SetString(py_get_module_interface_error_class(),
                    "Internal error while getting size of the parameters sequence");

                return nullptr;
            }
        } else {
            auto msg_str = std::string("The object does not provide the sequence protocol: ")
                + py_object_get_typename(params);

            PyErr_SetString(py_get_module_interface_error_class(), msg_str.c_str());
            return nullptr;
        }
    }

    try {
        py_parameter_set py_params(size, params);
        self->m_statement->execute(query, py_params);
    } catch (const ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }

    Py_RETURN_NONE;
}

PyObject* py_cursor_executemany(py_cursor* self, PyObject* args, PyObject* kwargs)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    static char *kwlist[] = {
        const_cast<char*>("query"),
        const_cast<char*>("params_list"),
        nullptr
    };

    const char* query = nullptr;
    PyObject *params_list = nullptr;

    int parsed = PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", kwlist, &query, &params_list);
    if (!parsed)
        return nullptr;

    Py_ssize_t size{0};
    Py_ssize_t row_size{0};
    if (params_list && params_list != Py_None) {
        if (PySequence_Check(params_list)) {
            size = PySequence_Size(params_list);
            if (size < 0) {
                PyErr_SetString(py_get_module_interface_error_class(),
                    "Internal error while getting the size of the parameter list sequence");

                return nullptr;
            }

            if (size > 0) {
                PyObject *row0 = PySequence_GetItem(params_list, 0);
                if (row0 == nullptr) {
                    PyErr_SetString(py_get_module_interface_error_class(),
                        "Can not get a first element of the parameter sequence");
                }

                if (!PySequence_Check(row0)) {
                    auto msg_str = std::string(
                        "A first element of the parameter sequence does not provide the sequence protocol: ")
                        + py_object_get_typename(params_list);

                    PyErr_SetString(py_get_module_interface_error_class(), msg_str.c_str());
                }

                row_size = PySequence_Size(row0);
                if (row_size < 0) {
                    PyErr_SetString(py_get_module_interface_error_class(),
                        "Internal error while getting size of the first parameter row");

                    return nullptr;
                }
            }
        } else {
            auto msg_str = std::string("The object does not provide the sequence protocol: ")
                + py_object_get_typename(params_list);

            PyErr_SetString(py_get_module_interface_error_class(), msg_str.c_str());
            return nullptr;
        }
    }

    try {
        py_parameter_set py_params(size, row_size, params_list);
        self->m_statement->execute(query, py_params);
    } catch (const ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }

    Py_RETURN_NONE;
}

PyObject* py_cursor_rowcount(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    return PyLong_FromLong(long(self->m_statement->get_affected_rows()));
}

PyObject* py_cursor_fetchone(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    auto &stmt = *self->m_statement;
    if (!stmt.is_executed()) {
        PyErr_SetString(py_get_module_interface_error_class(), "Query was not executed");
        return nullptr;
    }

    if (!stmt.is_data_available()) {
        Py_RETURN_NONE;
    }


    try {
        auto more_data = stmt.fetch_next_row();
        if (!more_data) {
            Py_RETURN_NONE;
        }
    } catch (ignite::ignite_error& err) {
        set_error(err);
        return nullptr;
    }

    auto row = stmt.get_current_row();
    auto res_list = PyTuple_New(row.size());
    if (!res_list) {
        PyErr_SetString(py_get_module_operational_error_class(), "Can not allocate a new list for the result set");
        return nullptr;
    }

    for (std::size_t i = 0; i < row.size(); ++i) {
        auto py_column = primitive_to_pyobject(row[i]);
        if (!py_column) {
            Py_DECREF(res_list);
            return nullptr;
        }
        PyTuple_SetItem(res_list, i, py_column);
    }

    return res_list;
}

PyObject* py_cursor_column_count(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    auto &stmt = *self->m_statement;
    if (!stmt.is_executed())
        return PyLong_FromLong(0);

    auto meta = stmt.get_meta();
    if (!meta) {
        return PyLong_FromLong(0);
    }

    return PyLong_FromLong(long(meta->size()));
}

const ignite::protocol::column_meta *get_meta_column(py_cursor* self, long idx, PyObject *&err_ret) {
    err_ret = nullptr;

    auto &stmt = *self->m_statement;
    if (!stmt.is_executed()) {
        Py_INCREF(Py_None);
        err_ret = Py_None;
        return nullptr;
    }

    auto meta = stmt.get_meta();
    if (!meta) {
        Py_INCREF(Py_None);
        err_ret = Py_None;
        return nullptr;
    }

    if (idx < 0 || idx >= long(meta->size())) {
        PyErr_SetString(py_get_module_interface_error_class(), "Column metadata index is out of bound");
        return nullptr;
    }

    return &meta->at(idx);
}

PyObject* py_cursor_column_name(py_cursor* self, PyObject* args)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyUnicode_FromStringAndSize(column->get_column_name().data(), column->get_column_name().size());
}

PyObject* py_cursor_column_type_code(py_cursor* self, PyObject* args)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyLong_FromLong(long(column->get_data_type()));
}

PyObject* py_cursor_column_display_size(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    Py_RETURN_NONE;
}

PyObject* py_cursor_column_internal_size(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    Py_RETURN_NONE;
}

PyObject* py_cursor_column_precision(py_cursor* self, PyObject* args)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyLong_FromLong(long(column->get_precision()));
}

PyObject* py_cursor_column_scale(py_cursor* self, PyObject* args)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyLong_FromLong(long(column->get_scale()));
}

PyObject* py_cursor_null_ok(py_cursor* self, PyObject* args)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyBool_FromLong(long(column->get_nullability() == ignite::protocol::nullability::NULLABLE));
}

PyTypeObject py_cursor_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    EXT_MODULE_NAME "." PY_CURSOR_CLASS_NAME
};

PyMethodDef py_cursor_methods[] = {
    // Core methods
    {"close", PyCFunction(py_cursor_close), METH_NOARGS, nullptr},
    {"execute", PyCFunction(py_cursor_execute), METH_VARARGS | METH_KEYWORDS, nullptr},
    {"executemany", PyCFunction(py_cursor_executemany), METH_VARARGS | METH_KEYWORDS, nullptr},
    {"rowcount", PyCFunction(py_cursor_rowcount), METH_NOARGS, nullptr},
    {"fetchone", PyCFunction(py_cursor_fetchone), METH_NOARGS, nullptr},
    // Column metadata retrieval methods
    {"column_count", PyCFunction(py_cursor_column_count), METH_NOARGS, nullptr},
    {"column_name", PyCFunction(py_cursor_column_name), METH_VARARGS, nullptr},
    {"column_type_code", PyCFunction(py_cursor_column_type_code), METH_VARARGS, nullptr},
    {"column_display_size", PyCFunction(py_cursor_column_display_size), METH_VARARGS, nullptr},
    {"column_internal_size", PyCFunction(py_cursor_column_internal_size), METH_VARARGS, nullptr},
    {"column_precision", PyCFunction(py_cursor_column_precision), METH_VARARGS, nullptr},
    {"column_scale", PyCFunction(py_cursor_column_scale), METH_VARARGS, nullptr},
    {"column_null_ok", PyCFunction(py_cursor_null_ok), METH_VARARGS, nullptr},
    {nullptr, nullptr, 0, nullptr}
};

} // anonymous namespace

int prepare_py_cursor_type() {
    py_cursor_type.tp_new = PyType_GenericNew;
    py_cursor_type.tp_basicsize = sizeof(py_cursor);
    py_cursor_type.tp_dealloc = destructor(py_cursor_dealloc);
    py_cursor_type.tp_flags = Py_TPFLAGS_DEFAULT;
    py_cursor_type.tp_methods = py_cursor_methods;
    py_cursor_type.tp_init = initproc(py_cursor_init);

    return PyType_Ready(&py_cursor_type);
}

int register_py_cursor_type(PyObject* mod) {
    auto res = PyModule_AddObject(mod, PY_CURSOR_CLASS_NAME, reinterpret_cast<PyObject *>(&py_cursor_type));
    if (res < 0) {
        Py_DECREF(reinterpret_cast<PyObject *>(&py_cursor_type));
    }
    return res;
}

py_cursor *make_py_cursor(std::unique_ptr<statement> stmt) {
    py_cursor* py_cursor_obj  = PyObject_New(py_cursor, &py_cursor_type);
    if (!py_cursor_obj)
        return nullptr;

    py_cursor_obj->m_statement = stmt.release();

    return py_cursor_obj;
}
