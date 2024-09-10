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

#include <optional>

#include <Python.h>

/**
 * Interface to handle Python sequences in a common manner.
 * Const. Non-owning.
 */
class py_sequence {
    /** GetItem function type. */
    typedef PyObject*(get_item_func_type)(PyObject *, Py_ssize_t);

public:
    /**
     * Constructor.
     *
     * @param seq Sequence.
     */
    static std::optional<py_sequence> make(PyObject *seq)
    {
        ptrdiff_t size{0};
        get_item_func_type *get_item_func{nullptr};
        if (seq) {
            if (PyTuple_Check(seq)) {
                size = PyTuple_Size(seq);
                if (size < 0) {
                    PyErr_SetString(PyExc_RuntimeError, "Internal error while getting size of the parameters tuple");
                    return std::nullopt;
                }
                get_item_func = &PyTuple_GetItem;
            } else if (PyList_Check(seq)) {
                size = PyList_Size(seq);
                if (size < 0) {
                    PyErr_SetString(PyExc_RuntimeError, "Internal error while getting size of the parameters list");
                    return std::nullopt;
                }
                get_item_func = &PyList_GetItem;
            } else {
                PyErr_SetString(PyExc_RuntimeError, "Only tuples and lists of parameters are supported");
                return std::nullopt;
            }
        }
        return {{seq, size, get_item_func}};
    }

    /**
     * Get sequence size.
     *
     * @return Size.
     */
    [[nodiscard]] Py_ssize_t get_size() const { return m_size; }

    /**
     * Get item.
     *
     * @param idx Item index.
     * @return An item.
     */
    [[nodiscard]] PyObject* get_item(Py_ssize_t idx) const { return m_get_item_func(m_seq, idx); }

private:
    /**
     * Constructor.
     *
     * @param seq Sequence.
     * @param size Size of the sequence.
     * @param func Element getter function.
     */
    py_sequence(PyObject *seq, Py_ssize_t size, get_item_func_type *func)
            : m_size(size)
            , m_seq(seq)
            , m_get_item_func(func) {}

    /** Sequence itself. */
    PyObject *m_seq{nullptr};

    /** Size. */
    Py_ssize_t m_size{0};

    /** Get item function. */
    get_item_func_type *m_get_item_func{nullptr};
};
