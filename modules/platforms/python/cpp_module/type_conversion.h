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

#include <ignite/protocol/writer.h>
#include <ignite/common/ignite_type.h>
#include <ignite/common/primitive.h>

#include <optional>

#include <Python.h>


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

static void write_pyobject(ignite::protocol::writer &writer, PyObject *obj) {
}
