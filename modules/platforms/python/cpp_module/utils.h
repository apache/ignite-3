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

#include <ignite/common/bytes_view.h>
#include <ignite/common/ignite_date.h>
#include <ignite/common/ignite_time.h>
#include <ignite/common/ignite_date_time.h>
#include <ignite/common/ignite_timestamp.h>
#include <ignite/common/ignite_duration.h>
#include <ignite/common/big_decimal.h>

#include <Python.h>

#define MODULE_NAME "pyignite3"
#define EXT_MODULE_NAME "_pyignite3_extension"

#define PY_ASSERT(cond, err)                                \
    do {                                                    \
        if (!(cond)) {                                      \
            PyErr_SetString(PyExc_AssertionError, (err));   \
            return nullptr;                                 \
        }                                                   \
    } while (false)


namespace ignite {
class diagnosable;
}

/**
 * Check odbc object for errors, and set a proper Python exception, if there are.
 * @param diag Diagnosable object instance.
 * @return @c true if there is no error, and @c false, if there is an error.
 */
bool check_errors(ignite::diagnosable& diag);

/**
 * Get a typename of the PyObject instance safely, if possible.
 *
 * @param obj Object.
 * @return Typename if available, and "Unknown" otherwise.
 */
const char* py_object_get_typename(PyObject* obj);

/**
 * Get the the module instance.
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
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return UUID class.
 */
PyObject* py_get_module_uuid_class();

/**
 * Get module's DATE class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return DATE class.
 */
PyObject* py_get_module_date_class();

/**
 * Get module's TIME class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return TIME class.
 */
PyObject* py_get_module_time_class();

/**
 * Get module's DATETIME class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return DATETIME class.
 */
PyObject* py_get_module_datetime_class();

/**
 * Get module's TIMESTAMP class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return TIMESTAMP class.
 */
PyObject* py_get_module_timestamp_class();

/**
 * Get module's NUMBER class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return NUMBER class.
 */
PyObject* py_get_module_number_class();

/**
 * Get module's DURATION class instance.
 * Faster then loading the class the ordinary way as the class is cached.
 *
 * @return DURATION class.
 */
PyObject* py_get_module_duration_class();

/**
 * Create a new instance of pyignite3.UUID from an array of bytes.
 *
 * @param value Uuid.
 * @return A new instance of pyignite3.UUID.
 */
PyObject* py_create_uuid(ignite::bytes_view bytes);

/**
 * Create a new instance of pyignite3.DATE from an ignite_date.
 *
 * @param value Date.
 * @return A new instance of pyignite3.DATE.
 */
PyObject* py_create_date(const ignite::ignite_date &value);

/**
 * Create a new instance of pyignite3.TIME from an ignite_date.
 *
 * @param value Time.
 * @return A new instance of pyignite3.TIME.
 */
PyObject* py_create_time(const ignite::ignite_time &value);

/**
 * Create a new instance of pyignite3.DATETIME from an ignite_date.
 *
 * @param value Date-Time.
 * @return A new instance of pyignite3.DATETIME.
 */
PyObject* py_create_datetime(const ignite::ignite_date_time &value);

/**
 * Create a new instance of pyignite3.DATETIME from an ignite_timestamp.
 *
 * @param value Timestamp.
 * @return A new instance of pyignite3.DATETIME.
 */
PyObject* py_create_datetime(const ignite::ignite_timestamp &value);

/**
 * Create a new instance of pyignite3.NUMBER from a string.
 *
 * @param value String representation of the decimal value.
 * @return A new instance of pyignite3.NUMBER.
 */
PyObject* py_create_number(std::string_view value);

/**
 * Create a new instance of pyignite3.DURATION from an ignite_duration.
 *
 * @param value ignite_duration.
 * @return A new instance of pyignite3.DURATION.
 */
PyObject* py_create_timedelta(const ignite::ignite_duration &value);
