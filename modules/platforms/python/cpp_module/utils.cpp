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

#define LAZY_INIT_MODULE_CLASS(class_name)          \
    static PyObject* instance{nullptr};             \
    if (!instance)                                  \
        instance = py_get_module_class(class_name); \
    return instance

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

PyObject* py_get_module() {
    static PyObject* instance{nullptr};
    // No need for sync here - Python is single-threaded
    if (!instance) {
        instance = PyImport_ImportModule(MODULE_NAME);
    }
    return instance;
}

PyObject* py_get_class(const char* module_name, const char* class_name) {
    auto module_obj = PyImport_ImportModule(module_name);
    if (!module_obj)
        return nullptr;

    auto class_obj = PyObject_GetAttrString(module_obj, class_name);
    Py_DECREF(module_obj);

    return class_obj;
}

PyObject* py_get_module_class(const char* class_name) {
    auto pyignite3_mod = py_get_module();
    if (!pyignite3_mod)
        return nullptr;

    return PyObject_GetAttrString(pyignite3_mod, class_name);
}

PyObject* py_call_method_no_arg(PyObject* obj, const char* method_name) {
    PyObject* py_method_name = PyUnicode_FromString(method_name);
    if (!py_method_name)
        return nullptr;
    auto py_method_name_guard = ignite::detail::defer([&]{ Py_DECREF(py_method_name); });

    return PyObject_CallMethodObjArgs(obj, py_method_name, nullptr);
}

PyObject* py_get_module_uuid_class() {
    LAZY_INIT_MODULE_CLASS("UUID");
}

PyObject* py_get_module_date_class() {
    LAZY_INIT_MODULE_CLASS("DATE");
}

PyObject* py_get_module_time_class() {
    LAZY_INIT_MODULE_CLASS("TIME");
}

PyObject* py_get_module_datetime_class() {
    LAZY_INIT_MODULE_CLASS("DATETIME");
}

PyObject* py_get_module_number_class() {
    LAZY_INIT_MODULE_CLASS("NUMBER");
}

PyObject* py_get_module_duration_class() {
    LAZY_INIT_MODULE_CLASS("DURATION");
}

PyObject* py_create_uuid(ignite::bytes_view bytes) {
    auto uuid_class = py_get_module_uuid_class();
    if (!uuid_class)
        return nullptr;

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
    auto date_class = py_get_module_date_class();
    if (!date_class)
        return nullptr;

    PyObject* year = PyLong_FromLong(value.get_year());
    if (!year)
        return nullptr;
    auto year_guard = ignite::detail::defer([&]{ Py_DECREF(year); });

    PyObject* month = PyLong_FromLong(value.get_month());
    if (!month)
        return nullptr;
    auto month_guard = ignite::detail::defer([&]{ Py_DECREF(month); });

    PyObject* day = PyLong_FromLong(value.get_day_of_month());
    if (!day)
        return nullptr;
    auto day_guard = ignite::detail::defer([&]{ Py_DECREF(day); });

    auto args = PyTuple_Pack(3, year, month, day);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    year_guard.release();
    month_guard.release();
    day_guard.release();

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    return PyObject_Call(date_class, args, kwargs);
}

PyObject* py_create_time(const ignite::ignite_time &value) {
    auto time_class = py_get_module_time_class();
    if (!time_class)
        return nullptr;

    PyObject* hour = PyLong_FromLong(value.get_hour());
    if (!hour)
        return nullptr;
    auto hour_guard = ignite::detail::defer([&]{ Py_DECREF(hour); });

    PyObject* minute = PyLong_FromLong(value.get_minute());
    if (!minute)
        return nullptr;
    auto minute_guard = ignite::detail::defer([&]{ Py_DECREF(minute); });

    PyObject* second = PyLong_FromLong(value.get_second());
    if (!second)
        return nullptr;
    auto second_guard = ignite::detail::defer([&]{ Py_DECREF(second); });

    PyObject* u_second = PyLong_FromLong(value.get_nano() / 1000);
    if (!u_second)
        return nullptr;
    auto u_second_guard = ignite::detail::defer([&]{ Py_DECREF(u_second); });

    auto args = PyTuple_Pack(4, hour, minute, second, u_second);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    hour_guard.release();
    minute_guard.release();
    second_guard.release();
    u_second_guard.release();

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    return PyObject_Call(time_class, args, kwargs);
}

PyObject* py_create_datetime(const ignite::ignite_date_time &value) {
    auto datetime_class = py_get_module_datetime_class();
    if (!datetime_class)
        return nullptr;
    auto class_guard = ignite::detail::defer([&]{ Py_DECREF(datetime_class); });

    PyObject* year = PyLong_FromLong(value.get_year());
    if (!year)
        return nullptr;
    auto year_guard = ignite::detail::defer([&]{ Py_DECREF(year); });

    PyObject* month = PyLong_FromLong(value.get_month());
    if (!month)
        return nullptr;
    auto month_guard = ignite::detail::defer([&]{ Py_DECREF(month); });

    PyObject* day = PyLong_FromLong(value.get_day_of_month());
    if (!day)
        return nullptr;
    auto day_guard = ignite::detail::defer([&]{ Py_DECREF(day); });

    PyObject* hour = PyLong_FromLong(value.get_hour());
    if (!hour)
        return nullptr;
    auto hour_guard = ignite::detail::defer([&]{ Py_DECREF(hour); });

    PyObject* minute = PyLong_FromLong(value.get_minute());
    if (!minute)
        return nullptr;
    auto minute_guard = ignite::detail::defer([&]{ Py_DECREF(minute); });

    PyObject* second = PyLong_FromLong(value.get_second());
    if (!second)
        return nullptr;
    auto second_guard = ignite::detail::defer([&]{ Py_DECREF(second); });

    PyObject* u_second = PyLong_FromLong(value.get_nano() / 1000);
    if (!u_second)
        return nullptr;
    auto u_second_guard = ignite::detail::defer([&]{ Py_DECREF(u_second); });

    auto args = PyTuple_Pack(7, year, month, day, hour, minute, second, u_second);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    year_guard.release();
    month_guard.release();
    day_guard.release();
    hour_guard.release();
    minute_guard.release();
    second_guard.release();
    u_second_guard.release();

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    return PyObject_Call(datetime_class, args, kwargs);
}

PyObject* py_create_datetime(const ignite::ignite_timestamp &value) {
    // TODO: Cache classes and functions for re-use
    auto datetime_class = py_get_module_datetime_class();
    if (!datetime_class)
        return nullptr;

    PyObject* second = PyLong_FromLongLong(value.get_epoch_second());
    if (!second)
        return nullptr;
    auto second_guard = ignite::detail::defer([&]{ Py_DECREF(second); });

    PyObject* u_second = PyLong_FromLongLong(value.get_nano() / 1000);
    if (!u_second)
        return nullptr;
    auto u_second_guard = ignite::detail::defer([&]{ Py_DECREF(u_second); });

    PyObject* constructor_name = PyUnicode_FromString("fromtimestamp");
    if (!constructor_name)
        return nullptr;
    auto constructor_name_guard = ignite::detail::defer([&]{ Py_DECREF(constructor_name); });

    return PyObject_CallMethodObjArgs(datetime_class, constructor_name, second, nullptr);
}

PyObject* py_create_number(std::string_view value) {
    auto number_class = py_get_module_number_class();
    if (!number_class)
        return nullptr;

    PyObject* str_obj = PyUnicode_FromStringAndSize(value.data(), value.size());
    if (!str_obj)
        return nullptr;
    auto str_obj_guard = ignite::detail::defer([&]{ Py_DECREF(str_obj); });

    auto args = PyTuple_Pack(1, str_obj);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    str_obj_guard.release();

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    return PyObject_Call(number_class, args, kwargs);
}

PyObject* py_create_timedelta(const ignite::ignite_duration &value) {
    auto duration_class = py_get_module_duration_class();
    if (!duration_class)
        return nullptr;

    auto args = PyTuple_New(0);
    if (!args)
        return nullptr;
    auto args_guard = ignite::detail::defer([&]{ Py_DECREF(args); });

    PyObject* second = PyLong_FromLong(value.get_seconds());
    if (!second)
        return nullptr;
    auto second_guard = ignite::detail::defer([&]{ Py_DECREF(second); });

    PyObject* u_second = PyLong_FromLong(value.get_nano() / 1000);
    if (!u_second)
        return nullptr;
    auto u_second_guard = ignite::detail::defer([&]{ Py_DECREF(u_second); });

    auto kwargs = PyDict_New();
    if (!kwargs)
        return nullptr;
    auto kwargs_guard = ignite::detail::defer([&]{ Py_DECREF(kwargs); });

    if (PyDict_SetItemString(kwargs, "seconds", second) < 0)
        return nullptr;
    second_guard.release();

    if (PyDict_SetItemString(kwargs, "microseconds", u_second) < 0)
        return nullptr;
    u_second_guard.release();

    return PyObject_Call(duration_class, args, kwargs);
}
