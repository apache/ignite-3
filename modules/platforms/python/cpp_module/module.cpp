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
#include "py_string.h"
#include "utils.h"

#include <ignite/odbc/sql_environment.h>
#include <ignite/odbc/sql_connection.h>
#include <ignite/common/detail/defer.h>

#include <memory>

#include <Python.h>

static PyObject* make_connection(std::unique_ptr<ignite::sql_environment> env,
    std::unique_ptr<ignite::sql_connection> conn)
{
    auto conn_class = py_get_module_class("Connection");
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

static PyObject* pyignite_dbapi_connect(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        "address",
        "identity",
        "secret",
        "schema",
        "timezone",
        "timeout",
        "page_size",
        "autocommit",
        "use_ssl",
        "ssl_keyfile",
        "ssl_certfile",
        "ssl_ca_certfile",
        nullptr
    };

    PyObject *address = nullptr;
    const char *identity = nullptr;
    const char *secret = nullptr;
    const char *schema = nullptr;
    const char *timezone = nullptr;
    int timeout = 0;
    int page_size = 0;
    int autocommit = 1;
    int use_ssl = 0;
    const char *ssl_keyfile = nullptr;
    const char *ssl_certfile = nullptr;
    const char *ssl_ca_certfile = nullptr;

    int parsed = PyArg_ParseTupleAndKeywords(args, kwargs, "O|$ssssiippsss", kwlist, &address, &identity, &secret,
        &schema, &timezone, &timeout, &page_size, &autocommit, &use_ssl, &ssl_keyfile, &ssl_certfile, &ssl_ca_certfile);

    if (!parsed)
        return nullptr;

    std::stringstream address_builder;
    if (PyList_Check(address)) {
        auto size = PyList_Size(address);
        for (Py_ssize_t idx = 0; idx < size; ++idx) {
            auto item = PyList_GetItem(address, idx);
            if (!PyUnicode_Check(item)) {
                PyErr_SetString(py_get_module_interface_error_class(),
                    "Only a string or a list of strings are allowed in 'address' parameter");

                return nullptr;
            }

            auto item_str = py_string::try_from_py_utf8(item);
            if (!item_str) {
                PyErr_SetString(py_get_module_interface_error_class(), "Can not convert address string to UTF-8");
                return nullptr;
            }

            address_builder << *item_str;
            if ((idx + 1) < size) {
                address_builder << ',';
            }
        }
    } else if (PyUnicode_Check(address)) {
        auto item_str = py_string::try_from_py_utf8(address);
        if (!item_str) {
            PyErr_SetString(py_get_module_interface_error_class(), "Can not convert address string to UTF-8");
            return nullptr;
        }
        address_builder << *item_str;
    } else {
        PyErr_SetString(py_get_module_interface_error_class(),
            "Only a string or a list of strings are allowed in 'address' parameter");
        return nullptr;
    }

    auto addrs_str = address_builder.str();
    if (addrs_str.empty()) {
        PyErr_SetString(py_get_module_interface_error_class(), "No addresses provided to connect");
        return nullptr;
    }

    using namespace ignite;

    auto sql_env = std::make_unique<sql_environment>();

    std::unique_ptr<sql_connection> sql_conn{sql_env->create_connection()};
    if (!check_errors(*sql_env))
        return nullptr;

    configuration cfg;
    cfg.set_address(addrs_str);

    if (schema)
        cfg.set_schema(schema);

    if (identity)
        cfg.set_auth_identity(identity);

    if (secret)
        cfg.set_auth_secret(secret);

    if (page_size)
        cfg.set_page_size(std::int32_t(page_size));

    if (timeout) {
        void* ptr_timeout = (void*)(ptrdiff_t(timeout));
        sql_conn->set_attribute(SQL_ATTR_CONNECTION_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return nullptr;

        sql_conn->set_attribute(SQL_ATTR_LOGIN_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return nullptr;
    }

    if (use_ssl)
        cfg.set_ssl_mode(ssl_mode_t::REQUIRE);

    if (ssl_keyfile)
        cfg.set_ssl_key_file(ssl_keyfile);

    if (ssl_certfile)
        cfg.set_ssl_cert_file(ssl_certfile);

    if (ssl_ca_certfile)
        cfg.set_ssl_ca_file(ssl_ca_certfile);

    sql_conn->establish(cfg);
    if (!check_errors(*sql_conn))
        return nullptr;

    if (!autocommit) {
        void* ptr_autocommit = (void*)(ptrdiff_t(SQL_AUTOCOMMIT_OFF));
        sql_conn->set_attribute(SQL_ATTR_AUTOCOMMIT, ptr_autocommit, 0);
        if (!check_errors(*sql_conn))
            return nullptr;
    }

    return make_connection(std::move(sql_env), std::move(sql_conn));
}

static PyMethodDef methods[] = {
    {"connect", (PyCFunction)pyignite_dbapi_connect, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr}       /* Sentinel */
};

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    EXT_MODULE_NAME,
    nullptr,                /* m_doc */
    -1,                     /* m_size */
    methods,                /* m_methods */
    nullptr,                /* m_slots */
    nullptr,                /* m_traverse */
    nullptr,                /* m_clear */
    nullptr,                /* m_free */
};

PyMODINIT_FUNC PyInit__pyignite_dbapi_extension(void) { // NOLINT(*-reserved-identifier)
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