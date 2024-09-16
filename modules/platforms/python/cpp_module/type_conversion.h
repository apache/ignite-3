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

#include "utils.h"

#include <ignite/protocol/writer.h>
#include <ignite/common/ignite_type.h>
#include <ignite/common/primitive.h>
#include <ignite/common/detail/defer.h>
#include <ignite/common/detail/bytes.h>
#include <ignite/tuple/binary_tuple_builder.h>
#include <ignite/protocol/utils.h>

#include <optional>
#include <sstream>

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
            auto &i8_val = value.get<std::int8_t>();
            return PyLong_FromLong(long(i8_val));
        }

        case ignite_type::INT16: {
            auto &i16_val = value.get<std::int16_t>();
            return PyLong_FromLong(long(i16_val));
        }

        case ignite_type::INT32: {
            auto &i32_val = value.get<std::int32_t>();
            return PyLong_FromLong(long(i32_val));
        }

        case ignite_type::INT64: {
            auto &i64_val = value.get<std::int64_t>();
            return PyLong_FromLongLong(i64_val);
        }

        case ignite_type::FLOAT: {
            auto &float_val = value.get<float>();
            return PyFloat_FromDouble(float_val);
        }

        case ignite_type::DOUBLE: {
            auto &double_val = value.get<double>();
            return PyFloat_FromDouble(double_val);
        }

        case ignite_type::BOOLEAN: {
            auto &bool_val = value.get<bool>();
            if (bool_val) {
                Py_RETURN_TRUE;
            } else {
                Py_RETURN_FALSE;
            }
        }

        case ignite_type::BYTE_ARRAY: {
            auto &blob_val = value.get<std::vector<std::byte>>();
            return PyBytes_FromStringAndSize((const char*)blob_val.data(), blob_val.size());
        }

        case ignite_type::UUID: {
            auto &uuid_val = value.get<ignite::uuid>();
            std::byte buf[16];
            ignite::detail::bytes::store<ignite::detail::endian::BIG>(buf, uuid_val.get_most_significant_bits());
            ignite::detail::bytes::store<ignite::detail::endian::BIG>(buf + 8, uuid_val.get_least_significant_bits());
            return py_create_uuid({buf, sizeof(buf)});
        }

        case ignite_type::DATE: {
            auto &date_val = value.get<ignite::ignite_date>();
            return py_create_date(date_val);
        }

        case ignite_type::TIME: {
            auto &time_val = value.get<ignite::ignite_time>();
            return py_create_time(time_val);
        }

        case ignite_type::DATETIME: {
            auto &datetime_val = value.get<ignite::ignite_date_time>();
            return py_create_datetime(datetime_val);
        }

        case ignite_type::TIMESTAMP: {
            auto &timestamp_val = value.get<ignite::ignite_timestamp>();
            return py_create_datetime(timestamp_val);
        }

        case ignite_type::DECIMAL: {
            auto &decimal_val = value.get<ignite::big_decimal>();
            std::stringstream converter;
            converter << decimal_val;
            auto str = converter.str();
            return py_create_number(str);
        }

        case ignite_type::DURATION: {
            auto &duration_val = value.get<ignite::ignite_duration>();
            return py_create_timedelta(duration_val);
        }

        case ignite_type::PERIOD:{
            PyErr_SetString(PyExc_RuntimeError, "PERIOD data type is not supported");
            return nullptr;
        }

        default: {
            auto err_msg = "The type is not supported: " + std::to_string(int(value.get_type()));
            PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());
            return nullptr;
        }
    }
}

static void submit_pyobject(ignite::binary_tuple_builder &builder, PyObject *obj, bool claim) {
    if (obj == Py_None) {
        if (claim) {
            builder.claim_null();
            builder.claim_null();
            builder.claim_null();
        } else {
            builder.append_null();
            builder.append_null();
            builder.append_null();
        }
        return;
    }

//    NUMBER = decimal.Decimal
//    DATE = datetime.date
//    TIME = datetime.time
//    DATETIME = datetime.datetime
//    DURATION = datetime.timedelta

    if (PyObject_IsInstance(obj, py_get_module_uuid_class())) {
        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::UUID);
            builder.claim_uuid();
            return;
        }

        auto py_bytes = PyObject_GetAttrString(obj, "bytes");
        if (!py_bytes) {
            throw ignite::ignite_error("Can not convert UUID to bytes");
        }
        auto py_bytes_guard = ignite::detail::defer([&] { Py_DECREF(py_bytes); });

        char* data = nullptr;
        Py_ssize_t len{0};
        if (PyBytes_AsStringAndSize(py_bytes, &data, &len) < 0) {
            throw ignite::ignite_error("Can not get data from UUID's bytes representation");
        }

        if (len != 16) {
            throw ignite::ignite_error("Unexpected size of the UUID's bytes data");
        }

        auto bytes = reinterpret_cast<std::byte*>(data);
        auto most = ignite::detail::bytes::load<ignite::detail::endian::BIG, std::int64_t>(bytes);
        auto least = ignite::detail::bytes::load<ignite::detail::endian::BIG, std::int64_t>(bytes + 8);

        ignite::uuid value(most, least);
        ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::UUID);
        builder.append_uuid(value);

        return;
    }

    if (PyUnicode_Check(obj)) {
        auto str_array = PyUnicode_AsUTF8String(obj);
        if (!str_array) {
            throw ignite::ignite_error("Can not convert string to UTF-8");
        }
        // To be called when the scope is left.
        auto str_array_guard = ignite::detail::defer([&] { Py_DECREF(str_array); });

        auto *data = PyBytes_AsString(str_array);
        auto len = PyBytes_Size(str_array);
        std::string_view view(data, len);

        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::STRING);
            builder.claim_varlen(view);
        } else {
            ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::STRING);
            builder.append_varlen(view);
        }
        return;
    }

    if (PyBytes_Check(obj)) {
        auto *data = reinterpret_cast<std::byte*>(PyBytes_AsString(obj));
        auto len = PyBytes_Size(obj);
        ignite::bytes_view view(data, len);

        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::BYTE_ARRAY);
            builder.claim_varlen(view);
        } else {
            ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::BYTE_ARRAY);
            builder.append_varlen(view);
        }
        return;
    }

    if (PyBool_Check(obj)) {
        bool val = (obj == Py_True);
        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::BOOLEAN);
            builder.claim_bool(val);
        } else {
            ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::BOOLEAN);
            builder.append_bool(val);
        }
        return;
    }

    if (PyFloat_Check(obj)) {
        double val = PyFloat_AsDouble(obj);
        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::DOUBLE);
            builder.claim_double(val);
        } else {
            ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::DOUBLE);
            builder.append_double(val);
        }
        return;
    }

    if (PyLong_Check(obj)) {
        auto val = PyLong_AsLongLong(obj);
        if (claim) {
            ignite::protocol::claim_type_and_scale(builder, ignite::ignite_type::INT64);
            builder.claim_int64(val);
        } else {
            ignite::protocol::append_type_and_scale(builder, ignite::ignite_type::INT64);
            builder.append_int64(val);
        }
        return;
    }

    // TODO: IGNITE-22745 Provide wider data types support
    throw ignite::ignite_error("Type is not supported: " + std::string(py_object_get_typename(obj)));
}
