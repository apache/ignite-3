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
#include "py_connection.h"
#include "py_cursor.h"

#include <ignite/odbc/sql_environment.h>
#include <ignite/odbc/sql_connection.h>
#include <ignite/common/detail/defer.h>

#include <memory>
#include <cmath>

#include <Python.h>


static PyObject* make_connection(std::unique_ptr<ignite::sql_environment> env,
    std::unique_ptr<ignite::sql_connection> conn)
{
    auto pyignite3_mod = PyImport_ImportModule("pyignite3");

    if (!pyignite3_mod)
        return nullptr;

    auto conn_class = PyObject_GetAttrString(pyignite3_mod, "Connection");
    Py_DECREF(pyignite3_mod);

    if (!conn_class)
        return nullptr;

    auto args = PyTuple_New(0);
    auto kwargs = Py_BuildValue("{}");
    PyObject* conn_obj  = PyObject_Call(conn_class, args, kwargs);
    Py_DECREF(conn_class);
    Py_DECREF(args);
    Py_DECREF(kwargs);

    if (!conn_obj)
        return nullptr;

    auto py_conn = make_py_connection(std::move(env), std::move(conn));
    if (!py_conn)
        return nullptr;

    auto res = PyObject_SetAttrString(conn_obj, "_py_connection", (PyObject*)py_conn);
    if (res)
        return nullptr;

    return conn_obj;
}

static PyObject* pyignite3_connect(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        "address",
        "identity",
        "secret",
        "schema",
        "timezone",
        "page_size",
        "timeout",
        nullptr
    };

    PyObject *address = nullptr;
    const char *identity = nullptr;
    const char *secret = nullptr;
    const char *schema = nullptr;
    const char *timezone = nullptr;
    int timeout = 0;
    int page_size = 0;

    int parsed = PyArg_ParseTupleAndKeywords(
        args, kwargs, "O|$ssssii", kwlist, &address, &identity, &secret, &schema, &timezone, &timeout, &page_size);

    if (!parsed)
        return nullptr;

    std::stringstream address_builder;
    if (PyList_Check(address)) {
        auto size = PyList_Size(address);
        for (Py_ssize_t idx = 0; idx < size; ++idx) {
            auto item = PyList_GetItem(address, idx);
            if (!PyUnicode_Check(item)) {
                PyErr_SetString(PyExc_RuntimeError, "Only list of string values is allowed in 'address' parameter");
                return nullptr;
            }

            auto str_array = PyUnicode_AsUTF8String(item);
            if (!str_array) {
                PyErr_SetString(PyExc_RuntimeError, "Can not convert address string to UTF-8");
                return nullptr;
            }
            // To be called when the scope is left.
            ignite::detail::defer([&] { Py_DECREF(str_array); });

            auto *data = PyBytes_AsString(str_array);
            auto len = PyBytes_Size(str_array);
            std::string_view view(data, len);

            address_builder << view;
            if ((idx + 1) < size) {
                address_builder << ',';
            }
        }
    }

    using namespace ignite;

    auto sql_env = std::make_unique<sql_environment>();

    std::unique_ptr<sql_connection> sql_conn{sql_env->create_connection()};
    if (!check_errors(*sql_env))
        return nullptr;

    configuration cfg;
    auto addrs_str = address_builder.str();
    cfg.set_address(addrs_str);

    if (schema)
        cfg.set_schema(schema);

    if (identity)
        cfg.set_auth_identity(identity);

    if (secret)
        cfg.set_auth_secret(secret);

    if (page_size)
        cfg.set_page_size(std::int32_t(page_size));

    if (timeout)
    {
        void* ptr_timeout = (void*)(ptrdiff_t(timeout));
        sql_conn->set_attribute(SQL_ATTR_CONNECTION_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return nullptr;

        sql_conn->set_attribute(SQL_ATTR_LOGIN_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return nullptr;
    }

    sql_conn->establish(cfg);
    if (!check_errors(*sql_conn))
        return nullptr;

    return make_connection(std::move(sql_env), std::move(sql_conn));
}

static PyMethodDef methods[] = {
    {"connect", (PyCFunction)pyignite3_connect, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr}       /* Sentinel */
};

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    nullptr,                /* m_doc */
    -1,                     /* m_size */
    methods,                /* m_methods */
    nullptr,                /* m_slots */
    nullptr,                /* m_traverse */
    nullptr,                /* m_clear */
    nullptr,                /* m_free */
};

PyMODINIT_FUNC PyInit__pyignite3_extension(void) { // NOLINT(*-reserved-identifier)
    PyObject* mod;

    mod = PyModule_Create(&module_def);
    if (mod == nullptr)
        return nullptr;

    if (prepare_py_connection_type() || prepare_py_cursor_type())
        return nullptr;

    if (register_py_connection_type(mod) || register_py_cursor_type(mod))
        return nullptr;

    return mod;
}

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
