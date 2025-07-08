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

#include <memory>

#include <Python.h>

#define PY_CURSOR_CLASS_NAME "PyCursor"

class statement;

/**
 * Cursor Python object.
 */
struct py_cursor {
    PyObject_HEAD

    /** Statement. */
    statement *m_statement;
};

/**
 * Create a new instance of py_cursor python class.
 *
 * @param stmt Statement.
 * @return A new class instance.
 */
py_cursor* make_py_cursor(std::unique_ptr<statement> stmt);

/**
 * Prepare PyCursor type for registration.
 */
int prepare_py_cursor_type();

/**
 * Register PyCursor type within module.
 */
int register_py_cursor_type(PyObject* mod);
