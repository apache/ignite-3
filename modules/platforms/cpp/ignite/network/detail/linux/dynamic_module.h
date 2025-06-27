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

#include <string>

namespace ignite::network
{

/**
 * Represents dynamically loadable program module such as dynamic or shared library.
 */
class dynamic_module
{
public:
    /**
     * Default constructor.
     */
    dynamic_module() = default;

    /**
     * Handle constructor.
     *
     * @param handle Os-specific module handle.
     */
    dynamic_module(void* handle) : m_handle(handle) {}

    /**
     * Load symbol from module.
     *
     * @param name Name of the symbol to load.
     * @return Pointer to symbol if found and NULL otherwise.
     */
    void* find_symbol(const char* name);

    /**
     * Load symbol from module.
     *
     * @param name Name of the symbol to load.
     * @return Pointer to symbol if found and NULL otherwise.
     */
    void* find_symbol(const std::string& name)
    {
        return find_symbol(name.c_str());
    }

    /**
     * Check if the instance is loaded.
     *
     * @return True if the instance is loaded.
     */
    bool is_loaded() const;

    /**
     * Unload module.
     */
    void unload();

private:
    void* m_handle{nullptr};
};

/**
 * Load module by the specified path.
 *
 * @param path Path to the module to load.
 * @return Module instance.
 */
dynamic_module load_module(const char* path);

/**
 * Load module by the specified path.
 *
 * @param path Path to the module to load.
 * @return Module instance.
 */
dynamic_module load_module(const std::string& path);

/**
 * Returns module associated with the calling process itself.
 *
 * @return Module for the calling process.
 */
dynamic_module get_current();

}
