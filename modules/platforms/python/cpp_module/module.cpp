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

#include <ignite/odbc/sql_environment.h>
#include <ignite/odbc/sql_connection.h>

#include <memory>
#include <cmath>

#include <Python.h>


PyObject* connect(PyObject* self, PyObject *args, PyObject* kwargs);

PyDoc_STRVAR(connect_doc,
"connect(address[, timeout, identity, secret, page_size, schema, timezone])\n"
"\n"
"Opens a connection to the Ignite Cluster.");

static PyMethodDef methods[] = {
    {"connect", (PyCFunction) connect, METH_VARARGS | METH_KEYWORDS, connect_doc},
    {NULL, NULL, 0, NULL}       /* Sentinel */
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_pyignite3_extension",
    NULL,                 /* m_doc */
    -1,                   /* m_size */
    methods,              /* m_methods */
    NULL,                 /* m_slots */
    NULL,                 /* m_traverse */
    NULL,                 /* m_clear */
    NULL,                 /* m_free */
};


PyMODINIT_FUNC PyInit__pyignite3_extension(void) {
	return PyModule_Create(&moduledef);
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

    // TODO: Set a proper error here, not a standard one.
    PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());

    return false;
}

static PyObject* connect(PyObject* self, PyObject* args, PyObject* kwargs) {
    static char *kwlist[] = {
        "address",
        "identity",
        "secret",
        "schema",
        "timezone",
        "page_size",
        "timeout",
        NULL
    };

    const char* address = NULL;
    const char* identity = NULL;
    const char* secret = NULL;
    const char* schema = NULL;
    const char* timezone = NULL;
    double timeout = 0.0;
    int page_size = 0;

    int parsed = PyArg_ParseTupleAndKeywords(
        args, kwargs, "s|ssssdi", kwlist, &address, &identity, &secret, &schema, &timezone, &timeout, &page_size);

    if (!parsed)
        return NULL;

    using namespace ignite;

    auto sql_env = std::make_unique<sql_environment>();

    std::unique_ptr<sql_connection> sql_conn{sql_env->create_connection()};
    if (!check_errors(*sql_env))
        return NULL;

    configuration cfg;
    cfg.set_address(address);

    if (schema)
        cfg.set_schema(schema);

    if (identity)
        cfg.set_auth_identity(identity);

    if (secret)
        cfg.set_auth_secret(secret);

    if (page_size)
        cfg.set_page_size(std::int32_t(page_size));

    std::int32_t ms_timeout = std::lround(timeout * 1000.0);
    if (ms_timeout)
    {
        void* ptr_timeout = (void*)(ptrdiff_t(ms_timeout));
        sql_conn->set_attribute(SQL_ATTR_CONNECTION_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return NULL;

        sql_conn->set_attribute(SQL_ATTR_LOGIN_TIMEOUT, ptr_timeout, 0);
        if (!check_errors(*sql_conn))
            return NULL;
    }

    sql_conn->establish(cfg);
    if (!check_errors(*sql_conn))
        return NULL;

    return NULL;
}


