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

#include <dlfcn.h>

namespace ignite::network
{

void* dynamic_module::find_symbol(const char* name)
{
    return dlsym(m_handle, name);
}

bool dynamic_module::is_loaded() const
{
    return m_handle != nullptr;
}

void dynamic_module::unload()
{
    if (is_loaded())
        dlclose(m_handle);
}

dynamic_module load_module(const char* path)
{
    void* handle = dlopen(path, RTLD_NOW);

    return dynamic_module(handle);
}

dynamic_module load_module(const std::string& path)
{
    return load_module(path.c_str());
}

dynamic_module get_current()
{
    return load_module(NULL);
}

}
