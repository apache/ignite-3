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

#include <ignite/common/detail/defer.h>

#include <Python.h>

#define LAZY_INIT_MODULE_CLASS(class_name)          \
    static PyObject* instance{nullptr};             \
    if (!instance)                                  \
        instance = py_get_module_class(class_name); \
    return instance


void set_error(const ignite::ignite_error &error) {
    auto error_class = py_get_module_interface_error_class();

    switch (error.get_status_code()) {
        case ignite::error::code::NULLABLE_VALUE:
        case ignite::error::code::CURSOR_ALREADY_CLOSED:
        case ignite::error::code::ILLEGAL_ARGUMENT: {
            error_class = py_get_module_interface_error_class();
            break;
        }

        case ignite::error::code::RESOURCE_CLOSING:
        case ignite::error::code::NODE_LEFT:
        case ignite::error::code::COMPONENT_NOT_STARTED:
        case ignite::error::code::NODE_STOPPING: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::SSL_CONFIGURATION:
        case ignite::error::code::USER_OBJECT_SERIALIZATION:
        case ignite::error::code::INTERNAL: {
            error_class = py_get_module_programming_error_class();
            break;
        }

        case ignite::error::code::TABLE_ALREADY_EXISTS:
        case ignite::error::code::TABLE_NOT_FOUND:
        case ignite::error::code::COLUMN_ALREADY_EXISTS:
        case ignite::error::code::COLUMN_NOT_FOUND:
        case ignite::error::code::SCHEMA_VERSION_MISMATCH:
        case ignite::error::code::UNSUPPORTED_PARTITION_TYPE: {
            error_class = py_get_module_programming_error_class();
            break;
        }

        case ignite::error::code::CONNECTION:
        case ignite::error::code::PROTOCOL:
        case ignite::error::code::PROTOCOL_COMPATIBILITY:
        case ignite::error::code::TABLE_ID_NOT_FOUND:
        case ignite::error::code::CONFIGURATION:
        case ignite::error::code::CLUSTER_ID_MISMATCH:
        case ignite::error::code::CLIENT_SSL_CONFIGURATION:
        case ignite::error::code::HANDSHAKE_HEADER:
        case ignite::error::code::SERVER_TO_CLIENT_REQUEST: {
            error_class = py_get_module_operational_error_class();
            break;
        }

        case ignite::error::code::QUERY_NO_RESULT_SET:
        case ignite::error::code::SCHEMA_NOT_FOUND:
        case ignite::error::code::STMT_PARSE:
        case ignite::error::code::STMT_VALIDATION:
        case ignite::error::code::EXECUTION_CANCELLED:
        case ignite::error::code::RUNTIME:
        case ignite::error::code::MAPPING:
        case ignite::error::code::TX_CONTROL_INSIDE_EXTERNAL_TX: {
            error_class = py_get_module_programming_error_class();
            break;
        }

        case ignite::error::code::CONSTRAINT_VIOLATION: {
            error_class = py_get_module_integrity_error_class();
            break;
        }

        case ignite::error::code::STARTING_STORAGE:
        case ignite::error::code::RESTORING_STORAGE:
        case ignite::error::code::COMPACTION:
        case ignite::error::code::OP_EXECUTION:
        case ignite::error::code::OP_EXECUTION_TIMEOUT:
        case ignite::error::code::COMPACTED:
        case ignite::error::code::DIVERGED: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::INDEX_NOT_FOUND:
        case ignite::error::code::INDEX_ALREADY_EXISTS: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::TX_STATE_STORAGE:
        case ignite::error::code::TX_STATE_STORAGE_STOPPED:
        case ignite::error::code::TX_UNEXPECTED_STATE:
        case ignite::error::code::ACQUIRE_LOCK:
        case ignite::error::code::ACQUIRE_LOCK_TIMEOUT:
        case ignite::error::code::TX_COMMIT:
        case ignite::error::code::TX_ROLLBACK:
        case ignite::error::code::TX_FAILED_READ_WRITE_OPERATION:
        case ignite::error::code::TX_STATE_STORAGE_REBALANCE:
        case ignite::error::code::TX_READ_ONLY_TOO_OLD:
        case ignite::error::code::TX_INCOMPATIBLE_SCHEMA:
        case ignite::error::code::TX_PRIMARY_REPLICA_EXPIRED:
        case ignite::error::code::TX_ALREADY_FINISHED:
        case ignite::error::code::TX_STALE_OPERATION:
        case ignite::error::code::TX_STALE_READ_ONLY_OPERATION:
        case ignite::error::code::TX_ALREADY_FINISHED_WITH_TIMEOUT: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::REPLICA_COMMON:
        case ignite::error::code::REPLICA_IS_ALREADY_STARTED:
        case ignite::error::code::REPLICA_TIMEOUT:
        case ignite::error::code::REPLICA_UNSUPPORTED_REQUEST:
        case ignite::error::code::REPLICA_UNAVAILABLE:
        case ignite::error::code::REPLICA_MISS:
        case ignite::error::code::CURSOR_CLOSE:
        case ignite::error::code::REPLICA_STOPPING: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::INDEX_NOT_BUILT:
        case ignite::error::code::STORAGE_CORRUPTED: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::ZONE_NOT_FOUND: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::UNRESOLVABLE_CONSISTENT_ID:
        case ignite::error::code::BIND:
        case ignite::error::code::FILE_TRANSFER:
        case ignite::error::code::FILE_VALIDATION:
        case ignite::error::code::RECIPIENT_LEFT:
        case ignite::error::code::ADDRESS_UNRESOLVED: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::CONFIG_READ:
        case ignite::error::code::CONFIG_FILE_CREATE:
        case ignite::error::code::CONFIG_WRITE:
        case ignite::error::code::CONFIG_PARSE: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::UNIT_NOT_FOUND:
        case ignite::error::code::UNIT_ALREADY_EXISTS:
        case ignite::error::code::UNIT_CONTENT_READ:
        case ignite::error::code::UNIT_UNAVAILABLE:
        case ignite::error::code::UNIT_ZIP:
        case ignite::error::code::UNIT_WRITE:
        case ignite::error::code::UNIT_NON_UNIQUE_FILENAMES: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::CLOSED: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::UNSUPPORTED_AUTHENTICATION_TYPE:
        case ignite::error::code::INVALID_CREDENTIALS:
        case ignite::error::code::BASIC_PROVIDER: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::CLASS_PATH:
        case ignite::error::code::CLASS_LOADER:
        case ignite::error::code::CLASS_INITIALIZATION:
        case ignite::error::code::QUEUE_OVERFLOW:
        case ignite::error::code::COMPUTE_JOB_STATUS_TRANSITION:
        case ignite::error::code::CANCELLING:
        case ignite::error::code::RESULT_NOT_FOUND:
        case ignite::error::code::FAIL_TO_GET_JOB_STATE:
        case ignite::error::code::COMPUTE_JOB_FAILED:
        case ignite::error::code::PRIMARY_REPLICA_RESOLVE:
        case ignite::error::code::CHANGE_JOB_PRIORITY:
        case ignite::error::code::NODE_NOT_FOUND:
        case ignite::error::code::MARSHALLING_TYPE_MISMATCH:
        case ignite::error::code::COMPUTE_JOB_CANCELLED:
        case ignite::error::code::RESOURCE_NOT_FOUND:
        case ignite::error::code::COMPUTE_PLATFORM_EXECUTOR: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::VALIDATION: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::PRIMARY_REPLICA_AWAIT_TIMEOUT:
        case ignite::error::code::PRIMARY_REPLICA_AWAIT: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::SYSTEM_WORKER_BLOCKED:
        case ignite::error::code::SYSTEM_CRITICAL_OPERATION_TIMEOUT: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::ILLEGAL_PARTITION_ID:
        case ignite::error::code::NODES_NOT_FOUND:
        case ignite::error::code::PARTITION_STATE:
        case ignite::error::code::CLUSTER_NOT_IDLE: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::CLUSTER_NOT_INITIALIZED:
        case ignite::error::code::CLUSTER_INIT_FAILED:
        case ignite::error::code::NODE_NOT_STARTED:
        case ignite::error::code::NODE_START: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::COMMON:
        case ignite::error::code::UNSUPPORTED_OBJECT_TYPE:
        case ignite::error::code::UNMARSHALLING: {
            error_class = py_get_module_not_supported_error_class();
            break;
        }

        case ignite::error::code::CLUSTER_NOT_INIT: {
            error_class = py_get_module_database_error_class();
            break;
        }

        case ignite::error::code::UNSUPPORTED_TABLE_BASED_REPLICATION:
        case ignite::error::code::OPERATION_TIMEOUT:
        case ignite::error::code::TX_DELAYED_ACK:
        case ignite::error::code::GROUP_OVERLOADED:
        case ignite::error::code::GROUP_UNAVAILABLE:
        case ignite::error::code::EMPTY_DATA_NODES:
        case ignite::error::code::JOIN_DENIED:
        case ignite::error::code::EMPTY_ASSIGNMENTS:
        case ignite::error::code::NOT_ENOUGH_ALIVE_NODES:
        case ignite::error::code::ILLEGAL_NODES_SET:
        case ignite::error::code::REQUEST_FORWARD:
        case ignite::error::code::REMOTE_NODE:
        case ignite::error::code::CONFIGURATION_APPLY:
        case ignite::error::code::CONFIGURATION_PARSE:
        case ignite::error::code::CONFIGURATION_VALIDATION: {
            error_class = py_get_module_not_supported_error_class();
            break;
        }
    }
    std::string error_str{error.what_str()};
    if (error.get_java_stack_trace()) {
        error_str += "\n" + *error.get_java_stack_trace();
    }
    PyErr_SetString(error_class, error_str.c_str());
}

std::string get_current_exception_as_string() {
    auto err_obj = PyErr_Occurred();
    if (!err_obj)
        return {};

    auto err_str_obj = PyObject_Str(err_obj);
    if (!err_str_obj)
        return "<Can not cast exception to a string>";

    auto *data = PyBytes_AsString(err_str_obj);
    auto len = PyBytes_Size(err_str_obj);
    return {data, std::size_t(len)};
}

const char* py_object_get_typename(const PyObject* obj) {
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
    auto pyignite_dbapi_mod = py_get_module();
    if (!pyignite_dbapi_mod)
        return nullptr;

    return PyObject_GetAttrString(pyignite_dbapi_mod, class_name);
}

PyObject* py_call_method_no_arg(PyObject* obj, const char* method_name) {
    PyObject* py_method_name = PyUnicode_FromString(method_name);
    if (!py_method_name)
        return nullptr;
    auto py_method_name_guard = ignite::detail::defer([&]{ Py_DECREF(py_method_name); });

    return PyObject_CallMethodObjArgs(obj, py_method_name, nullptr);
}

std::int64_t py_get_attr_int(PyObject* obj, const char* attr_name) {
    auto attr_obj = PyObject_GetAttrString(obj, attr_name);
    if (!attr_obj) {
        throw ignite::ignite_error(get_current_exception_as_string());
    }
    auto attr_obj_guard = ignite::detail::defer([&] { Py_DECREF(attr_obj); });
    if (PyErr_Occurred()) {
        throw ignite::ignite_error(get_current_exception_as_string());
    }
    auto res = PyLong_AsLongLong(attr_obj);
    if (PyErr_Occurred()) {
        throw ignite::ignite_error(get_current_exception_as_string());
    }
    return res;
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

PyObject* py_get_module_timestamp_class() {
    LAZY_INIT_MODULE_CLASS("TIMESTAMP");
}

PyObject* py_get_module_number_class() {
    LAZY_INIT_MODULE_CLASS("NUMBER");
}

PyObject* py_get_module_duration_class() {
    LAZY_INIT_MODULE_CLASS("DURATION");
}

PyObject* py_get_module_warning_class() {
    LAZY_INIT_MODULE_CLASS("Warning");
}

PyObject* py_get_module_interface_error_class() {
    LAZY_INIT_MODULE_CLASS("InterfaceError");
}

PyObject* py_get_module_database_error_class() {
    LAZY_INIT_MODULE_CLASS("DatabaseError");
}

PyObject* py_get_module_data_error_class() {
    LAZY_INIT_MODULE_CLASS("DataError");
}

PyObject* py_get_module_operational_error_class() {
    LAZY_INIT_MODULE_CLASS("OperationalError");
}

PyObject* py_get_module_integrity_error_class() {
    LAZY_INIT_MODULE_CLASS("IntegrityError");
}

PyObject* py_get_module_internal_error_class() {
    LAZY_INIT_MODULE_CLASS("InternalError");
}

PyObject* py_get_module_programming_error_class() {
    LAZY_INIT_MODULE_CLASS("ProgrammingError");
}

PyObject* py_get_module_not_supported_error_class() {
    LAZY_INIT_MODULE_CLASS("NotSupportedError");
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
