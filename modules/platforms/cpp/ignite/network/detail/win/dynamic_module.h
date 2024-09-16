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

#include "ignite/network/detail/sockets.h"

#include <string>
#include <filesystem>

namespace ignite::network
{

/**
 * Represents dynamically loadable program module such as a dynamic or a shared library.
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
     * @param handle Os-specific Module handle.
     */
    [[nodiscard]] explicit dynamic_module(HMODULE handle) : m_handle(handle) {}

    /**
     * Load symbol from Module.
     *
     * @param name Name of the symbol to load.
     * @return Pointer to symbol if found and NULL otherwise.
     */
    [[nodiscard]] void* find_symbol(const char* name);

    /**
     * Load symbol from Module.
     *
     * @param name Name of the symbol to load.
     * @return Pointer to symbol if found and NULL otherwise.
     */
    [[nodiscard]] void* find_symbol(const std::string& name)
    {
        return find_symbol(name.c_str());
    }

    /**
     * Check if the instance is loaded.
     *
     * @return True if the instance is loaded.
     */
    [[nodiscard]] bool is_loaded() const;

    /**
     * Unload module.
     */
    void unload();

private:
    /** Handle. */
    HMODULE m_handle{nullptr};
};

/**
 * Load Module by the specified path.
 *
 * @param path Path to the Module to load.
 * @return Module instance.
 */
[[nodiscard]] dynamic_module load_module(const char *path);

/**
 * Load Module by the specified path.
 *
 * @param path Path to the Module to load.
 * @return Module instance.
 */
[[nodiscard]] dynamic_module load_module(const std::string &path);

/**
 * Load Module by the specified path.
 *
 * @param path Path to the Module to load.
 * @return Module instance.
 */
[[nodiscard]] inline dynamic_module load_module(const std::filesystem::path &path)
{
    return load_module(path.string());
}

/**
 * Returns Module associated with the calling process itself.
 *
 * @return Module for the calling process.
 */
[[nodiscard]] dynamic_module get_current();

}


