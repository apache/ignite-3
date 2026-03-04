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

#include "py_object.h"

#include <string>

#include <Python.h>

class py_string : public py_object_based {
public:
    // Default
    py_string() = default;

    /**
     * Make a string from python object.
     *
     * @param obj Python Object.
     * @return A new instance of the class. Can be invalid if the object was not a valid utf-8 string.
     *
     * @warning Error is set if an invalid object is returned.
     */
    static py_string try_from_py_utf8(PyObject* obj) {
        py_object val{PyUnicode_AsUTF8String(obj)};
        if (!val)
            return {};

        return py_string{std::move(val)};
    }

    /**
     * Get string data.
     *
     * @return String data.
     */
    std::string_view get_data() const { return m_data; }

    /**
     * Get string data.
     *
     * @return String data.
     */
    std::string_view operator*() const { return get_data(); }

private:
    /**
     * Constructor.
     *
     * @param obj Object.
     */
    explicit py_string(py_object &&obj)
        : py_object_based(std::move(obj))
        , m_data(PyBytes_AsString(m_obj.get()), PyBytes_Size(m_obj.get())) {}

    /** The actual string data. */
    std::string_view m_data;
};
