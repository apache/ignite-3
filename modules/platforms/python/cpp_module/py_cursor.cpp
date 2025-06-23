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

#include "module.h"
#include "utils.h"
#include "py_cursor.h"
#include "type_conversion.h"

#include <Python.h>

/**
 * Write row of the param set using a provided writer.
 *
 * @param writer Writer.
 * @param params_row Parameter Row.
 * @param row_size_expected Expected size of the row.
 */
void write_row(ignite::protocol::writer &writer, PyObject *params_row, std::int32_t row_size_expected) {
    if (!params_row || params_row == Py_None) {
        throw ignite::ignite_error("Parameter row can not be None");
    }

    if (!PySequence_Check(params_row)) {
        throw ignite::ignite_error(std::string("Parameter row does not provide the sequence protocol: ") +
            py_object_get_typename(params_row));
    }

    Py_ssize_t seq_size{PySequence_Size(params_row)};
    if (seq_size < 0) {
        throw ignite::ignite_error("Internal error while getting size of the parameter list sequence");
    }

    auto row_size = std::int32_t(seq_size);
    if (row_size != row_size_expected) {
        throw ignite::ignite_error("Row size is unexpected: " + std::to_string(row_size) +
            ", expected row size: " + std::to_string(row_size_expected));
    }

    ignite::binary_tuple_builder row_builder{row_size * 3};
    row_builder.start();

    for (std::int32_t idx = 0; idx < row_size; ++idx) {
        submit_pyobject(row_builder, PySequence_GetItem(params_row, idx), true);
    }

    row_builder.layout();

    for (std::int32_t idx = 0; idx < row_size; ++idx) {
        submit_pyobject(row_builder, PySequence_GetItem(params_row, idx), false);
    }

    auto row_data = row_builder.build();
    writer.write_binary(row_data);
}

/**
 * Python parameter set.
 */
class py_parameter_set : public ignite::parameter_set {
public:
    /**
     * Constructor.
     *
     * @param size Size of the row.
     * @param params Python parameters sequence.
     */
    py_parameter_set(Py_ssize_t size, PyObject *params) : m_size(size), m_params(params) {}

    /**
     * Write only the first row of the param set using a provided writer.
     *
     * @param writer Writer.
     */
    void write(ignite::protocol::writer &writer) const override {
        if (!m_size) {
            writer.write_nil();
            return;
        }

        writer.write(std::int32_t(m_size));
        write_row(writer, m_params, std::int32_t(m_size));
    }

    /**
     * Write rows of the param set in an interval [begin, end) using a provided writer.
     *
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    void write(ignite::protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const override {
        throw ignite::ignite_error("Execution with the batch of parameters is not implemented");
    }

    /**
     * Get parameter set size.
     *
     * @return Number of rows in a set.
     */
    [[nodiscard]] std::int32_t get_param_set_size() const override {
        return 1;
    }

    /**
     * Set the number of parameters processed in batch.
     *
     * @param processed Processed.
     */
    void set_params_processed(SQLULEN processed) override { m_processed = processed; }

    /**
     * Get pointer to array in which to return the status of each set of parameters.
     *
     * @return Value.
     */
    [[nodiscard]] SQLUSMALLINT *get_params_status_ptr() const override {
        return nullptr;
    }

private:
    /** Size. */
    Py_ssize_t m_size{0};

    /** Python sequence of parameters. */
    PyObject *m_params{nullptr};

    /** Processed params. */
    SQLULEN m_processed{0};
};

/**
 * Python parameter list set.
 */
class py_parameter_list_set : public ignite::parameter_set {
public:
    /**
     * Constructor.
     *
     * @param size Size number of rows to insert.
     * @param row_size Number of params in a single row.
     * @param params Python parameter sequence list.
     */
    py_parameter_list_set(Py_ssize_t size, Py_ssize_t row_size, PyObject *params)
        : m_size(size)
        , m_row_size(row_size)
        , m_params(params) {}

    /**
     * Write only the first row of the param set using a provided writer.
     *
     * @param writer Writer.
     */
    void write(ignite::protocol::writer &writer) const override {
        PyObject *row = PySequence_GetItem(m_params, 0);
        if (!m_row_size) {
            writer.write_nil();
            return;
        }

        writer.write(std::int32_t(m_row_size));
        write_row(writer, row, std::int32_t(m_row_size));
    }

    /**
     * Write rows of the param set in interval [begin, end) using provided writer.
     *
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    void write(ignite::protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const override {
        Py_ssize_t interval_end = std::min(m_size, Py_ssize_t(end));
        std::int32_t rows_num = std::int32_t(interval_end) - std::int32_t(begin);

        writer.write(std::int32_t(m_row_size));
        writer.write(rows_num);
        writer.write_bool(last);

        for (auto i = Py_ssize_t(begin); i < interval_end; ++i) {
            PyObject *row = PySequence_GetItem(m_params, i);
            write_row(writer, row, std::int32_t(m_row_size));
        }
    }

    /**
     * Get parameter set size.
     *
     * @return Number of rows in a set.
     */
    [[nodiscard]] std::int32_t get_param_set_size() const override {
        return std::int32_t(m_size);
    }

    /**
     * Set the number of parameters processed in batch.
     *
     * @param processed Processed.
     */
    void set_params_processed(SQLULEN processed) override { m_processed = processed; }

    /**
     * Get pointer to array in which to return the status of each set of parameters.
     *
     * @return Value.
     */
    [[nodiscard]] SQLUSMALLINT *get_params_status_ptr() const override {
        return nullptr;
    }

private:
    /** Rows number. */
    Py_ssize_t m_size{0};

    /** Row size. */
    Py_ssize_t m_row_size{0};

    /** Python sequence of parameters. */
    PyObject *m_params{nullptr};

    /** Processed params. */
    SQLULEN m_processed{0};
};

/**
 * Check if the cursor is open. Set error if not.
 * @param self Cursor.
 * @return @c true if open and @c false, otherwise.
 */
static bool py_cursor_expect_open(const py_cursor* self) {
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

static PyObject* py_cursor_close(py_cursor* self, PyObject*)
{
    if (self->m_statement) {
        self->m_statement->close();
        if (!check_errors(*self->m_statement))
            return nullptr;

        delete self->m_statement;
        self->m_statement = nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* py_cursor_execute(py_cursor* self, PyObject* args, PyObject* kwargs)
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

    py_parameter_set py_params(size, params);
    self->m_statement->execute_sql_query(query, py_params);
    if (!check_errors(*self->m_statement))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_cursor_executemany(py_cursor* self, PyObject* args, PyObject* kwargs)
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
                    "Internal error while getting size of the parameter list sequence");

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

    py_parameter_list_set py_params_list(size, row_size, params_list);
    self->m_statement->execute_sql_query(query, py_params_list);
    if (!check_errors(*self->m_statement))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_cursor_rowcount(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    auto query = self->m_statement->get_query();

    if (!query)
        return PyLong_FromLong(-1);

    return PyLong_FromLong(long(query->affected_rows()));
}

static PyObject* py_cursor_fetchone(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    auto query = self->m_statement->get_query();
    if (!query) {
        PyErr_SetString(py_get_module_interface_error_class(), "Query was not executed");
        return nullptr;
    }

    if (query->get_type() != ignite::query_type::DATA) {
        auto err_msg = "Unexpected query type: " + std::to_string(int(query->get_type()));
        PyErr_SetString(py_get_module_interface_error_class(), err_msg.c_str());
        return nullptr;
    }

    if (!query->is_data_available()) {
        Py_RETURN_NONE;
    }

    auto& query0 = static_cast<ignite::data_query&>(*query);
    auto res = query0.fetch_next_row();
    if (res == ignite::sql_result::AI_NO_DATA) {
        Py_RETURN_NONE;
    }

    if (!check_errors(res, *self->m_statement)) {
        return nullptr;
    }

    auto row = query0.get_current_row();
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

static PyObject* py_cursor_column_count(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

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
        PyErr_SetString(py_get_module_interface_error_class(), "Column metadata index is out of bound");
        return nullptr;
    }

    return &meta->at(idx);
}

static PyObject* py_cursor_column_name(py_cursor* self, PyObject* args)
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

static PyObject* py_cursor_column_type_code(py_cursor* self, PyObject* args)
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

static PyObject* py_cursor_column_display_size(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_cursor_column_internal_size(py_cursor* self, PyObject*)
{
    if (!py_cursor_expect_open(self))
        return nullptr;

    Py_RETURN_NONE;
}

static PyObject* py_cursor_column_precision(py_cursor* self, PyObject* args)
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

static PyObject* py_cursor_column_scale(py_cursor* self, PyObject* args)
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

static PyObject* py_cursor_null_ok(py_cursor* self, PyObject* args)
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

    return PyBool_FromLong(long(column->get_nullability() == ignite::nullability::NULLABLE));
}

static PyTypeObject py_cursor_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    EXT_MODULE_NAME "." PY_CURSOR_CLASS_NAME
};

static struct PyMethodDef py_cursor_methods[] = {
    // Core methods
    {"close", (PyCFunction)py_cursor_close, METH_NOARGS, nullptr},
    {"execute", (PyCFunction)py_cursor_execute, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"executemany", (PyCFunction)py_cursor_executemany, METH_VARARGS | METH_KEYWORDS, nullptr},
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
