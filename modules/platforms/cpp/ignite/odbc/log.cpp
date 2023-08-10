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

#include "ignite/odbc/log.h"

#include <cstdlib>

namespace ignite {

log_stream::~log_stream() {
    if (m_logger)
        m_logger->write_message(m_string_buf.str());
}

odbc_logger::odbc_logger(const char *path, bool trace_enabled)
    : m_trace_enabled(trace_enabled) {
    if (path)
        m_stream.open(path);
}

bool odbc_logger::is_enabled() const {
    return m_stream.is_open();
}

void odbc_logger::write_message(std::string const &message) {
    if (is_enabled()) {
        std::lock_guard<std::mutex> guard(m_mutex);
        m_stream << message << std::endl;
    }
}

odbc_logger *odbc_logger::get() {
    const char *env_var_path = "IGNITE3_ODBC_LOG_PATH";
    const char *env_var_trace = "IGNITE3_ODBC_LOG_TRACE_ENABLED";
    static odbc_logger logger(getenv(env_var_path), getenv(env_var_trace) != nullptr);
    return logger.is_enabled() ? &logger : nullptr;
}

} // namespace ignite
