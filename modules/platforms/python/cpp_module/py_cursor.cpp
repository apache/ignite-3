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
#include <ignite/odbc/query/data_query.h>

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
    // TODO IGNITE-22741 Support parameters
    PyObject *params = nullptr;

    int parsed = PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", kwlist, &query, &params);

    if (!parsed)
        return nullptr;

    self->m_statement->execute_sql_query(query);
    if (!check_errors(*self->m_statement))
        return nullptr;

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* py_cursor_rowcount(py_cursor* self, PyObject*)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    auto query = self->m_statement->get_query();

    if (!query)
        return PyLong_FromLong(-1);

    return PyLong_FromLong(long(query->affected_rows()));
}

static PyObject* primitive_to_pyobject(ignite::primitive value) {
    using ignite::ignite_type;

    if (value.is_null()) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    switch (value.get_type()) {
        case ignite_type::STRING: {
            auto &str_val = value.get<std::string>();
            return PyUnicode_FromStringAndSize(str_val.c_str(), str_val.size());
        }

        case ignite_type::INT8: {
            auto &i8_val =  value.get<std::int8_t>();
            return PyLong_FromLong(long(i8_val));
        }

        case ignite_type::INT16: {
            auto &i16_val =  value.get<std::int16_t>();
            return PyLong_FromLong(long(i16_val));
        }

        case ignite_type::INT32: {
            auto &i32_val =  value.get<std::int32_t>();
            return PyLong_FromLong(long(i32_val));
        }

        case ignite_type::INT64: {
            auto &i64_val =  value.get<std::int64_t>();
            return PyLong_FromLongLong(i64_val);
        }

        case ignite_type::FLOAT: {
            auto &float_val =  value.get<float>();
            return PyFloat_FromDouble(float_val);
        }

        case ignite_type::DOUBLE: {
            auto &double_val =  value.get<double>();
            return PyFloat_FromDouble(double_val);
        }

        case ignite_type::BOOLEAN: {
            auto &bool_val =  value.get<bool>();
            if (bool_val) {
                Py_RETURN_TRUE;
            } else {
                Py_RETURN_FALSE;
            }
        }

        case ignite_type::BYTE_ARRAY: {
            auto &blob_val =  value.get<std::vector<std::byte>>();
            return PyBytes_FromStringAndSize((const char*)blob_val.data(), blob_val.size());
        }

        case ignite_type::UUID:
        case ignite_type::DATE:
        case ignite_type::TIMESTAMP:
        case ignite_type::TIME:
        case ignite_type::DATETIME:
        case ignite_type::BITMASK:
        case ignite_type::DECIMAL:
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
        case ignite_type::NUMBER:
        default: {
            // TODO: IGNITE-22745 Provide wider data types support
            auto err_msg = "The type is not supported yet: " + std::to_string(int(value.get_type()));
            PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());
            return nullptr;
        }
    }
}

static PyObject* py_cursor_fetchone(py_cursor* self, PyObject*)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    auto query = self->m_statement->get_query();
    if (!query) {
        PyErr_SetString(PyExc_RuntimeError, "Query was not executed");
        return nullptr;
    }

    if (query->get_type() != ignite::query_type::DATA) {
        auto err_msg = "Unexpected query type: " + std::to_string(int(query->get_type()));
        PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());
        return nullptr;
    }

    if (!query->is_data_available()) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    auto& query0 = static_cast<ignite::data_query&>(*query);
    auto res = query0.fetch_next_row();
    if (res == ignite::sql_result::AI_NO_DATA) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (!check_errors(*self->m_statement)) {
        return nullptr;
    }

    auto row = query0.get_current_row();
    auto res_list = PyList_New(row.size());
    if (!res_list) {
        PyErr_SetString(PyExc_RuntimeError, "Can not allocate a new list for the result set");
        return nullptr;
    }

    for (std::size_t i = 0; i < row.size(); ++i) {
        auto py_column = primitive_to_pyobject(row[i]);
        if (!py_column) {
            Py_DECREF(res_list);
            return nullptr;
        }
        PyList_SetItem(res_list, i, py_column);
    }

    return res_list;
}

static PyObject* py_cursor_column_count(py_cursor* self, PyObject*)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    auto query = self->m_statement->get_query();

    if (!query)
        return PyLong_FromLong(0);

    return PyLong_FromLong(long(query->get_meta()->size()));
}

const ignite::column_meta *get_meta_column(py_cursor* self, long idx, PyObject *&err_ret) {
    err_ret = nullptr;
    auto query = self->m_statement->get_query();
    if (!query) {
        Py_INCREF(Py_None);
        err_ret = Py_None;
        return nullptr;
    }

    auto meta = query->get_meta();
    if (!meta) {
        Py_INCREF(Py_None);
        err_ret = Py_None;
        return nullptr;
    }

    if (idx < 0 || idx >= long(meta->size())) {
        PyErr_SetString(PyExc_RuntimeError, "Column metadata index is out of bound");
        return nullptr;
    }

    return &meta->at(idx);
}

static PyObject* py_cursor_column_name(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

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

static PyObject* py_cursor_column_type_code(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

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

static PyObject* py_cursor_column_display_size(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* py_cursor_column_internal_size(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* py_cursor_column_precision(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

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

static PyObject* py_cursor_column_scale(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

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

static PyObject* py_cursor_null_ok(py_cursor* self, PyObject* args)
{
    if (!self->m_statement) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor is in invalid state (Already closed?)");
        return nullptr;
    }

    long idx{0};

    int parsed = PyArg_ParseTuple(args, "l", &idx);
    if (!parsed)
        return nullptr;

    PyObject* err{nullptr};
    auto column = get_meta_column(self, idx, err);
    if (!column)
        return err;

    return PyBool_FromLong(long(column->get_nullability() == ignite::nullability::NULLABLE));
}

static PyTypeObject py_cursor_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    MODULE_NAME "." PY_CURSOR_CLASS_NAME
};

static struct PyMethodDef py_cursor_methods[] = {
    // Core methods
    {"close", (PyCFunction)py_cursor_close, METH_NOARGS, nullptr},
    {"execute", (PyCFunction)py_cursor_execute, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"rowcount", (PyCFunction)py_cursor_rowcount, METH_NOARGS, nullptr},
    {"fetchone", (PyCFunction)py_cursor_fetchone, METH_NOARGS, nullptr},
    // Column metadata retrieval methods
    {"column_count", (PyCFunction)py_cursor_column_count, METH_NOARGS, nullptr},
    {"column_name", (PyCFunction)py_cursor_column_name, METH_VARARGS, nullptr},
    {"column_type_code", (PyCFunction)py_cursor_column_type_code, METH_VARARGS, nullptr},
    {"column_display_size", (PyCFunction)py_cursor_column_display_size, METH_VARARGS, nullptr},
    {"column_internal_size", (PyCFunction)py_cursor_column_internal_size, METH_VARARGS, nullptr},
    {"column_precision", (PyCFunction)py_cursor_column_precision, METH_VARARGS, nullptr},
    {"column_scale", (PyCFunction)py_cursor_column_scale, METH_VARARGS, nullptr},
    {"column_null_ok", (PyCFunction)py_cursor_null_ok, METH_VARARGS, nullptr},
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
    auto res = PyModule_AddObject(mod, PY_CURSOR_CLASS_NAME, (PyObject *)&py_cursor_type);
    if (res < 0) {
        Py_DECREF((PyObject *)&py_cursor_type);
    }
    return res;
}

py_cursor *make_py_cursor(std::unique_ptr<ignite::sql_statement> stmt) {
    py_cursor* py_cursor_obj  = PyObject_New(py_cursor, &py_cursor_type);
    if (!py_cursor_obj)
        return nullptr;

    py_cursor_obj->m_statement = stmt.release();

    return py_cursor_obj;
}
