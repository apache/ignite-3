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

#pragma once

#include <ignite_error.h>
#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/ignite_duration.h>

#include <Python.h>

#define PY_ASSERT(cond, err)                                \
    do {                                                    \
        if (!(cond)) {                                      \
            PyErr_SetString(PyExc_AssertionError, (err));   \
            return nullptr;                                 \
        }                                                   \
    } while (false)


namespace ignite {
enum class sql_result;
class diagnosable;
}

/**
 * Convert Ignite Error to DB API Error.
 *
 * @param error Ignite error.
 */
void set_error(const ignite::ignite_error &error);

/**
 * Get the current exception as a string.
 *
 * @return Current exception as a string.
 */
std::string get_current_exception_as_string();

/**
 * Get a typename of the PyObject instance safely, if possible.
 *
 * @param obj Object.
 * @return Typename if available, and "Unknown" otherwise.
 */
const char* py_object_get_typename(PyObject* obj);

/**
 * Get the module instance.
 *
 * @return Client's module.
 */
PyObject* py_get_module();

/**
 * Get a Python's class instance.
 *
 * @param module_name Module name.
 * @param class_name Class name.
 * @return
 */
PyObject* py_get_class(const char* module_name, const char* class_name);

/**
 * Get a Python's class instance from the Client's module.
 *
 * @param class_name Class name.
 * @return Client's module.
 */
PyObject* py_get_module_class(const char* class_name);

/**
 * Call a method with no args.
 *
 * @param obj Object.
 * @param method_name Method name.
 * @return Call result.
 */
PyObject* py_call_method_no_arg(PyObject* obj, const char* method_name);

/**
 * Get an integer attribute.
 *
 * @param obj Object.
 * @param attr_name Attribute name.
 * @return Call result.
 */
std::int64_t py_get_attr_int(PyObject* obj, const char* attr_name);

/**
 * Get module's UUID class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return UUID class.
 */
PyObject* py_get_module_uuid_class();

/**
 * Get module's DATE class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return DATE class.
 */
PyObject* py_get_module_date_class();

/**
 * Get module's TIME class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return TIME class.
 */
PyObject* py_get_module_time_class();

/**
 * Get module's DATETIME class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return DATETIME class.
 */
PyObject* py_get_module_datetime_class();

/**
 * Get module's TIMESTAMP class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return TIMESTAMP class.
 */
PyObject* py_get_module_timestamp_class();

/**
 * Get module's NUMBER class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return NUMBER class.
 */
PyObject* py_get_module_number_class();

/**
 * Get module's DURATION class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return DURATION class.
 */
PyObject* py_get_module_duration_class();

/**
 * Get module's Warning class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return Warning class.
 */
PyObject* py_get_module_warning_class();

/**
 * Get module's InterfaceError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return InterfaceError class.
 */
PyObject* py_get_module_interface_error_class();

/**
 * Get module's DatabaseError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return DatabaseError class.
 */
PyObject* py_get_module_database_error_class();

/**
 * Get module's DataError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return DataError class.
 */
PyObject* py_get_module_data_error_class();

/**
 * Get module's OperationalError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return OperationalError class.
 */
PyObject* py_get_module_operational_error_class();

/**
 * Get module's IntegrityError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return IntegrityError class.
 */
PyObject* py_get_module_integrity_error_class();

/**
 * Get module's InternalError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return InternalError class.
 */
PyObject* py_get_module_internal_error_class();

/**
 * Get module's ProgrammingError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return ProgrammingError class.
 */
PyObject* py_get_module_programming_error_class();

/**
 * Get module's NotSupportedError class instance.
 * Faster than loading the class the ordinary way as the class is cached.
 *
 * @return NotSupportedError class.
 */
PyObject* py_get_module_not_supported_error_class();

/**
 * Create a new instance of pyignite_dbapi.UUID from an array of bytes.
 *
 * @param bytes A buffer containing a UUID.
 * @return A new instance of pyignite_dbapi.UUID.
 */
PyObject* py_create_uuid(ignite::bytes_view bytes);

/**
 * Create a new instance of pyignite_dbapi.DATE from an ignite_date.
 *
 * @param value Date.
 * @return A new instance of pyignite_dbapi.DATE.
 */
PyObject* py_create_date(const ignite::ignite_date &value);

/**
 * Create a new instance of pyignite_dbapi.TIME from an ignite_date.
 *
 * @param value Time.
 * @return A new instance of pyignite_dbapi.TIME.
 */
PyObject* py_create_time(const ignite::ignite_time &value);

/**
 * Create a new instance of pyignite_dbapi.DATETIME from an ignite_date.
 *
 * @param value Date-Time.
 * @return A new instance of pyignite_dbapi.DATETIME.
 */
PyObject* py_create_datetime(const ignite::ignite_date_time &value);

/**
 * Create a new instance of pyignite_dbapi.DATETIME from an ignite_timestamp.
 *
 * @param value Timestamp.
 * @return A new instance of pyignite_dbapi.DATETIME.
 */
PyObject* py_create_datetime(const ignite::ignite_timestamp &value);

/**
 * Create a new instance of pyignite_dbapi.NUMBER from a string.
 *
 * @param value String representation of the decimal value.
 * @return A new instance of pyignite_dbapi.NUMBER.
 */
PyObject* py_create_number(std::string_view value);

/**
 * Create a new instance of pyignite_dbapi.DURATION from an ignite_duration.
 *
 * @param value ignite_duration.
 * @return A new instance of pyignite_dbapi.DURATION.
 */
PyObject* py_create_timedelta(const ignite::ignite_duration &value);
