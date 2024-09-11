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
 * Get a Python's class instance.
 *
 * @param module_name Module name.
 * @param class_name Class name.
 * @return
 */
PyObject* py_get_class(const char* module_name, const char* class_name);

/**
 * Create a new instance of pyignite3.UUID from an array of bytes.
 * @param value Uuid.
 * @return A new instance of pyignite3.UUID.
 */
PyObject* py_create_uuid(ignite::bytes_view bytes);
