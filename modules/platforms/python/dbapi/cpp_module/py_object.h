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

#include <Python.h>

/**
 * Safer wrapper of PyObject.
 */
class py_object {
public:
    // Default
    py_object() = default;

    // Delete.
    py_object(const py_object&) = delete;
    py_object& operator=(const py_object&) = delete;

    /**
     * Move constructor.
     *
     * @param another Another Object.
     */
    py_object(py_object&& another) noexcept
        : m_obj(another.m_obj) {
        another.m_obj = nullptr;
    }

    /**
     * Move operator.
     *
     * @param another Another Object.
     * @return This.
     */
    py_object& operator=(py_object&& another) noexcept {
        std::swap(m_obj, another.m_obj);
        another.reset();

        return *this;
    }

    /**
     * Basic constructor.
     *
     * @param obj Python Object.
     */
    py_object(PyObject* obj) : m_obj(obj) {}

    /**
     * Destructor.
     */
    ~py_object() {
        reset();
    }

    /**
     * Check whether the object is valid.
     */
    bool is_valid() const { return m_obj != nullptr; }

    /**
     * Check whether the object is valid.
     */
    operator bool() const { return is_valid(); }

    /**
     * Change the value of the python object.
     *
     * @param obj New value.
     */
    void reset(PyObject* obj = nullptr) {
        Py_XDECREF(m_obj);
        m_obj = obj;
    }

    /**
     * Get Pointer.
     */
    PyObject* get() { return m_obj; }

    /**
     * Get Pointer.
     */
    const PyObject* get() const { return m_obj; }

private:
    /** Reference to the python object. */
    PyObject* m_obj{nullptr};
};

/**
 * Class to be used to inherit the basic and safe behavior of py_object.
 */
class py_object_based {
public:
    py_object_based() = default;

    /**
     * Check whether the object is valid.
     */
    bool is_valid() const { return m_obj.is_valid(); }

    /**
     * Check whether the object is valid.
     */
    operator bool() const { return is_valid(); }

protected:
    /**
     * Constructor.
     *
     * @param obj Object.
     */
    explicit py_object_based(py_object &&obj) : m_obj(std::move(obj)) {}

    /** Object. */
    py_object m_obj{};
};

