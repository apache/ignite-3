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

#include <ignite/common/ignite_error.h>

#include <memory>
#include <functional>

namespace ignite::detail {

/**
 * Deferred call, to be called when the scope is left.
 * Useful for cleanup routines.
 */
class deferred_call
{
public:
    /**
     * Constructor.
     *
     * @param val Instance, to call method on.
     * @param method Method to call.
     */
    explicit deferred_call(std::function<void()> routine) : m_routine(std::move(routine)) { }

    /**
     * Destructor.
     */
    ~deferred_call()
    {
        if (m_routine)
            m_routine();
    }

    /**
     * Release the deferred_call instance, without calling the stored function.
     */
    void release()
    {
        m_routine = {};
    }

private:
    /** Function to call. */
    std::function<void()> m_routine;
};

/**
 * Factory to construct a deferred call.
 *
 * @param routine Function to defer.
 * @return A new instance of the deferred call.
 */
inline deferred_call defer(std::function<void()> routine) {
    return deferred_call(std::move(routine));
}

} // namespace ignite::detail
