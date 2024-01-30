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

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/tuple/binary_tuple_builder.h"

#include <algorithm>

namespace ignite {

void parameter_set::set_param_set_size(SQLULEN size) {
    m_param_set_size = size;
}

void parameter_set::bind_parameter(std::uint16_t param_idx, const parameter &param) {
    m_params[param_idx] = param;
}

void parameter_set::unbind_parameter(std::uint16_t param_idx) {
    m_params.erase(param_idx);
}

void parameter_set::unbind_all() {
    m_params.clear();
}

std::uint16_t parameter_set::get_parameters_number() const {
    return static_cast<std::uint16_t>(m_params.size());
}

void parameter_set::set_param_bind_offset_ptr(int *ptr) {
    m_param_bind_offset = ptr;
}

int *parameter_set::get_param_bind_offset_ptr() {
    return m_param_bind_offset;
}

void parameter_set::prepare() {
    m_param_set_pos = 0;

    for (auto &param : m_params)
        param.second.reset_stored_data();
}

bool parameter_set::is_data_at_exec_needed() const {
    return std::any_of(
        m_params.begin(), m_params.end(), [](const auto &param) { return !param.second.is_data_ready(); });
}

void parameter_set::set_params_processed_ptr(SQLULEN *ptr) {
    m_processed_param_rows = ptr;
}

SQLULEN *parameter_set::get_params_processed_ptr() const {
    return m_processed_param_rows;
}

void parameter_set::set_params_status_ptr(SQLUSMALLINT *value) {
    m_params_status = value;
}

SQLUSMALLINT *parameter_set::get_params_status_ptr() const {
    return m_params_status;
}

void parameter_set::set_params_status(int64_t idx, SQLUSMALLINT status) const {
    if (idx < 0 || !m_params_status || idx >= static_cast<int64_t>(m_param_set_size))
        return;

    m_params_status[idx] = status;
}

void parameter_set::set_params_processed(SQLULEN processed) const {
    if (m_processed_param_rows)
        *m_processed_param_rows = processed;
}

bool parameter_set::is_parameter_selected() const {
    return m_current_param_idx != 0;
}

parameter *parameter_set::get_parameter(std::uint16_t idx) {
    auto it = m_params.find(idx);
    if (it != m_params.end())
        return &it->second;

    return nullptr;
}

const parameter *parameter_set::get_parameter(std::uint16_t idx) const {
    auto it = m_params.find(idx);
    if (it != m_params.end())
        return &it->second;

    return nullptr;
}

parameter *parameter_set::get_selected_parameter() {
    return get_parameter(m_current_param_idx);
}

parameter *parameter_set::select_next_parameter() {
    for (auto it = m_params.begin(); it != m_params.end(); ++it) {
        std::uint16_t param_idx = it->first;
        parameter &param = it->second;

        if (!param.is_data_ready()) {
            m_current_param_idx = param_idx;
            return &param;
        }
    }

    return nullptr;
}

void parameter_set::write(protocol::writer &writer) const {
    auto args_num = calculate_row_len();
    if (args_num == 0) {
        writer.write_nil();
        return;
    }

    writer.write(args_num);

    write_row(writer, 0);
}

void parameter_set::write(protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const {
    std::int32_t row_len = calculate_row_len();

    writer.write(row_len);

    SQLULEN interval_end = std::min(m_param_set_size, end);

    assert(begin < interval_end);

    auto interval_len = static_cast<std::int32_t>(interval_end - begin);

    writer.write(interval_len);
    writer.write_bool(last);

    if (row_len) {
        for (SQLULEN i = begin; i < interval_end; ++i)
            write_row(writer, i);
    }
}

void parameter_set::write_row(protocol::writer &writer, SQLULEN idx) const {
    auto args_num = calculate_row_len();
    binary_tuple_builder row_builder{args_num * 3};

    row_builder.start();

    std::uint16_t prev = 0;

    int app_offset = m_param_bind_offset ? *m_param_bind_offset : 0;
    for (const auto &pair : m_params) {
        std::uint16_t param_idx = pair.first;
        const parameter &param = pair.second;

        while ((param_idx - prev) > 1) {
            row_builder.claim_null();
            ++prev;
        }

        param.claim(row_builder, app_offset, idx);

        prev = param_idx;
    }

    row_builder.layout();

    for (const auto &pair : m_params) {
        std::uint16_t param_idx = pair.first;
        const parameter &param = pair.second;

        while ((param_idx - prev) > 1) {
            row_builder.append_null();
            ++prev;
        }

        param.append(row_builder, app_offset, idx);

        prev = param_idx;
    }

    auto args_data = row_builder.build();
    writer.write_binary(args_data);
}

std::int32_t parameter_set::calculate_row_len() const {
    if (!m_params.empty())
        return static_cast<std::int32_t>(m_params.rbegin()->first);

    return 0;
}

std::int32_t parameter_set::get_param_set_size() const {
    return static_cast<std::int32_t>(m_param_set_size);
}

} // namespace ignite
