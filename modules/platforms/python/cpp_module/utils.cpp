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

#include "utils.h"

#include <ignite/odbc/diagnostic/diagnosable.h>
#include <ignite/common/detail/defer.h>

#include <Python.h>

bool check_errors(ignite::diagnosable& diag) {
    auto &records = diag.get_diagnostic_records();
    if (records.is_successful())
        return true;

    std::string err_msg;
    switch (records.get_return_code()) {
        case SQL_INVALID_HANDLE:
            err_msg = "Invalid object handle";
            break;

        case SQL_NO_DATA:
            err_msg = "No data available";
            break;

        case SQL_ERROR:
            auto record = records.get_status_record(1);
            err_msg = record.get_message_text();
            break;
    }

    // TODO: IGNITE-22226 Set a proper error here, not a standard one.
    PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());

    return false;
}

const char* py_object_get_typename(PyObject* obj) {
    if (!obj || !obj->ob_type || !obj->ob_type->tp_name) {
        return "Unknown";
    }

    return obj->ob_type->tp_name;
}

PyObject* py_get_class(const char* module_name, const char* class_name) {
    auto pyignite3_mod = PyImport_ImportModule(module_name);

    if (!pyignite3_mod)
        return nullptr;

    auto conn_class = PyObject_GetAttrString(pyignite3_mod, class_name);
    Py_DECREF(pyignite3_mod);

    return conn_class;
}

PyObject* py_create_uuid(ignite::bytes_view bytes) {
    auto uuid_class = py_get_class(MODULE_NAME, "UUID");
    if (!uuid_class)
        return nullptr;
    auto class_guard = ignite::detail::defer([&]{ Py_DECREF(uuid_class); });

    auto args = PyTuple_New(0);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    PyObject* py_bytes = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    if (!py_bytes)
        return nullptr;

    if (PyDict_SetItemString(kwargs, "bytes", py_bytes) < 0) {
        Py_DECREF(py_bytes);
        return nullptr;
    }

    return PyObject_Call(uuid_class, args, kwargs);
}

PyObject* py_create_date(const ignite::ignite_date &value) {
    auto uuid_class = py_get_class(MODULE_NAME, "DATE");
    if (!uuid_class)
        return nullptr;
    auto class_guard = ignite::detail::defer([&]{ Py_DECREF(uuid_class); });

    PyObject* year = PyLong_FromLong(value.get_year());
    if (!year)
        return nullptr;
    auto year_guard = ignite::detail::defer([&]{ Py_DECREF(year); });

    PyObject* month = PyLong_FromLong(value.get_month());
    if (!year)
        return nullptr;
    auto month_guard = ignite::detail::defer([&]{ Py_DECREF(month); });

    PyObject* day = PyLong_FromLong(value.get_day_of_month());
    if (!year)
        return nullptr;
    auto day_guard = ignite::detail::defer([&]{ Py_DECREF(month); });

    auto args = PyTuple_New(3);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    if (PyTuple_SetItem(args, 0, year) < 0)
        return nullptr;
    year_guard.release();

    if (PyTuple_SetItem(args, 1, month) < 0)
        return nullptr;
    month_guard.release();

    if (PyTuple_SetItem(args, 2, day) < 0)
        return nullptr;
    day_guard.release();

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    return PyObject_Call(uuid_class, args, kwargs);
}
