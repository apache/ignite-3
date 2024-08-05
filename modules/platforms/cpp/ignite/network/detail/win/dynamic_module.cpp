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

#include "dynamic_module.h"

#include <sstream>
#include <vector>

namespace {

std::wstring string_to_wstring(const std::string& str)
{
    int ws_len = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), NULL, 0);

    if (!ws_len)
        return {};

    std::vector<WCHAR> converted(ws_len);

    MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), &converted[0], ws_len);

    std::wstring res(converted.begin(), converted.end());

    return res;
}

} // anonymous namespace

namespace ignite::network {

void* dynamic_module::find_symbol(const char* name)
{
    return GetProcAddress(m_handle, name);
}

bool dynamic_module::is_loaded() const
{
    return m_handle != nullptr;
}

void dynamic_module::unload()
{
    if (is_loaded())
    {
        FreeLibrary(m_handle);

        m_handle = nullptr;
    }
}

dynamic_module load_module(const char* path)
{
    std::string str_path(path);

    return load_module(str_path);
}

dynamic_module load_module(const std::string& path)
{
    std::wstring converted_path = string_to_wstring(path);

    HMODULE handle = LoadLibrary(reinterpret_cast<LPCSTR>(converted_path.c_str()));

    return dynamic_module{handle};
}

dynamic_module get_current()
{
    HMODULE handle = GetModuleHandle(NULL);

    return dynamic_module{handle};
}

} // namespace ignite::network
