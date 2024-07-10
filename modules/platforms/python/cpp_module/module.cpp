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

#include <Python.h>

#ifdef _MSC_VER

typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;

#else
#include <stdint.h>
#endif

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
    "_pyignite3",
    NULL,                 /* m_doc */
    -1,                   /* m_size */
    methods,              /* m_methods */
    NULL,                 /* m_slots */
    NULL,                 /* m_traverse */
    NULL,                 /* m_clear */
    NULL,                 /* m_free */
};


PyMODINIT_FUNC PyInit__pyignite3(void) {
	return PyModule_Create(&moduledef);
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

    return NULL;
}


