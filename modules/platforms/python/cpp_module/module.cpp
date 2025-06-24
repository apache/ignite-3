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
#include "py_object.h"
#include "py_string.h"
#include "utils.h"

#include <ignite/common/detail/defer.h>

#include <sstream>

#include <Python.h>

static PyObject* make_connection()
{
    py_object conn_class(py_get_module_class("Connection"));
    if (!conn_class)
        return nullptr;

    py_object args(PyTuple_New(0));
    py_object kwargs(Py_BuildValue("{}"));
    return PyObject_Call(conn_class.get(), args.get(), kwargs.get());
}

static PyObject* make_connection(std::vector<ignite::end_point> addresses, const char* schema, const char* identity, const char* secret,
    int page_size, int timeout, bool autocommit) {
    auto py_conn = make_py_connection(std::move(addresses), schema, identity, secret, page_size, timeout, autocommit);
    if (!py_conn)
        return nullptr;

    auto conn_obj = make_connection();
    if (!conn_obj)
        return nullptr;

    if (PyObject_SetAttrString(conn_obj, "_py_connection", reinterpret_cast<PyObject *>(py_conn)))
        return nullptr;

    return conn_obj;
}

static PyObject* pyignite_dbapi_connect(PyObject*, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        const_cast<char*>("address"),
        const_cast<char*>("identity"),
        const_cast<char*>("secret"),
        const_cast<char*>("schema"),
        const_cast<char*>("timezone"),
        const_cast<char*>("timeout"),
        const_cast<char*>("page_size"),
        const_cast<char*>("autocommit"),
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

    std::vector<ignite::end_point> addresses;
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

            ignite::end_point ep;
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

    return make_connection(address_builder.str(), schema, identity, secret, page_size, timeout, autocommit != 0, ssl_keyfile, ssl_certfile, ssl_ca_certfile);
}

static PyMethodDef methods[] = {
    {"connect", PyCFunction(pyignite_dbapi_connect), METH_VARARGS | METH_KEYWORDS, nullptr},
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
    PyObject *mod = PyModule_Create(&module_def);
    if (mod == nullptr)
        return nullptr;

    if (prepare_py_connection_type() || prepare_py_cursor_type())
        return nullptr;

    if (register_py_connection_type(mod) || register_py_cursor_type(mod))
        return nullptr;

    return mod;
}