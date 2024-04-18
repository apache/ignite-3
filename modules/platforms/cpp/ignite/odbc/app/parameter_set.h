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

#include "ignite/common/ignite_type.h"
#include "ignite/odbc/app/parameter.h"
#include "ignite/protocol/writer.h"

#include <cstdint>
#include <map>

namespace ignite {

/**
 * Parameter set.
 */
class parameter_set {
    /** Parameter binging map type alias. */
    typedef std::map<std::uint16_t, parameter> parameter_binding_map;

public:
    /**
     * Default constructor.
     */
    parameter_set() = default;

    // Deleted
    parameter_set(parameter_set &&) = delete;
    parameter_set(const parameter_set &) = delete;
    parameter_set &operator=(parameter_set &&) = delete;
    parameter_set &operator=(const parameter_set &) = delete;

    /**
     * Set m_parameters set size.
     *
     * @param size Size of the parameter set.
     */
    void set_param_set_size(SQLULEN size);

    /**
     * Bind parameter.
     *
     * @param param_idx parameter index.
     * @param param parameter.
     */
    void bind_parameter(std::uint16_t param_idx, const parameter &param);

    /**
     * Unbind specified parameter.
     *
     * @param param_idx parameter index.
     */
    void unbind_parameter(std::uint16_t param_idx);

    /**
     * Unbind all m_parameters.
     */
    void unbind_all();

    /**
     * Get number of bound m_parameters.
     *
     * @return Number of bound m_parameters.
     */
    [[nodiscard]] std::uint16_t get_parameters_number() const;

    /**
     * Set parameter binding offset pointer.
     *
     * @param ptr parameter binding offset pointer.
     */
    void set_param_bind_offset_ptr(int *ptr);

    /**
     * Get parameter binding offset pointer.
     *
     * @return parameter binding offset pointer.
     */
    int *get_param_bind_offset_ptr();

    /**
     * Prepare m_parameters set for statement execution.
     */
    void prepare();

    /**
     * Check if the data at-execution is needed.
     *
     * @return True if the data at execution is needed.
     */
    [[nodiscard]] bool is_data_at_exec_needed() const;

    /**
     * Check if the parameter selected for putting data at-execution.
     *
     * @return True if the parameter selected for putting data at-execution.
     */
    [[nodiscard]] bool is_parameter_selected() const;

    /**
     * Get parameter by index.
     *
     * @param idx Index.
     * @return parameter or null, if parameter is not bound.
     */
    parameter *get_parameter(std::uint16_t idx);

    /**
     * Get parameter by index.
     *
     * @param idx Index.
     * @return parameter or null, if parameter is not bound.
     */
    const parameter *get_parameter(std::uint16_t idx) const;

    /**
     * Get selected parameter.
     *
     * @return parameter or null, if parameter is not bound.
     */
    parameter *get_selected_parameter();

    /**
     * Internally selects next parameter for putting data at-execution.
     *
     * @return parameter if found and null otherwise.
     */
    parameter *select_next_parameter();

    /**
     * Write only first row of the param set using provided writer.
     * @param writer Writer.
     */
    void write(protocol::writer &writer) const;

    /**
     * Write rows of the param set in interval [begin, end) using provided writer.
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    void write(protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const;

    /**
     * Calculate row length.
     *
     * @return Row length.
     */
    [[nodiscard]] std::int32_t calculate_row_len() const;

    /**
     * Get parameter set size.
     *
     * @return Number of rows in set.
     */
    [[nodiscard]] std::int32_t get_param_set_size() const;

    /**
     * Set number of m_parameters processed in batch.
     *
     * @param processed Processed.
     */
    void set_params_processed(SQLULEN processed) const;

    /**
     * Number of processed params should be written using provided address.
     *
     * @param ptr Pointer.
     */
    void set_params_processed_ptr(SQLULEN *ptr);

    /**
     * Get pointer to write number of m_parameters processed in batch.
     *
     * @return Pointer to write number of m_parameters processed in batch.
     */
    [[nodiscard]] SQLULEN *get_params_processed_ptr() const;

    /**
     * Set pointer to array in which to return the status of each
     * set of m_parameters.
     * @param value Value.
     */
    void set_params_status_ptr(SQLUSMALLINT *value);

    /**
     * Get pointer to array in which to return the status of each
     * set of m_parameters.
     * @return Value.
     */
    [[nodiscard]] SQLUSMALLINT *get_params_status_ptr() const;

    /**
     * Set parameter status.
     * @param idx parameter index.
     * @param status Status to set.
     */
    void set_params_status(int64_t idx, SQLUSMALLINT status) const;

private:
    /**
     * Write single row of the param set using provided writer.
     * @param writer Writer.
     * @param idx Row index.
     */
    void write_row(protocol::writer &writer, SQLULEN idx) const;

    /** Parameters. */
    parameter_binding_map m_params{};

    /** Offset added to pointers to change binding of m_parameters. */
    int *m_param_bind_offset{nullptr};

    /** Processed m_parameters. */
    SQLULEN *m_processed_param_rows{nullptr};

    /** Parameters status. */
    SQLUSMALLINT *m_params_status{nullptr};

    /** Parameter set size. */
    SQLULEN m_param_set_size{1};

    /** Current position in parameter set. */
    SQLULEN m_param_set_pos{0};

    /** Index of the parameter, which is currently being set. */
    std::uint16_t m_current_param_idx{0};
};

} // namespace ignite
