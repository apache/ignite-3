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

#include "ignite/odbc/app/parameter.h"
#include "ignite/protocol/writer.h"

#include <cstdint>
#include <map>

namespace ignite {

/**
 * Parameter set.
 */
class parameter_set {
public:
    /**
     * Default constructor.
     */
    parameter_set() = default;

    /**
     * Destructor.
     */
    virtual ~parameter_set() = default;

    /**
     * Write only the first row of the param set using a provided writer.
     *
     * @param writer Writer.
     */
    virtual void write(protocol::writer &writer) const = 0;

    /**
     * Write rows of the param set in an interval [begin, end) using a provided writer.
     *
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    virtual void write(protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const = 0;

    /**
     * Get parameter set size.
     *
     * @return Number of rows in a set.
     */
    [[nodiscard]] virtual std::int32_t get_param_set_size() const = 0;

    /**
     * Set the number of parameters processed in batch.
     *
     * @param processed Processed.
     */
    virtual void set_params_processed(SQLULEN processed) = 0;

    /**
     * Get pointer to array in which to return the status of each
     * set of parameters.
     *
     * @return Value.
     */
    [[nodiscard]] virtual SQLUSMALLINT *get_params_status_ptr() const = 0;
};


/**
 * Parameter set implementation.
 */
class parameter_set_impl : public parameter_set {
    /** Parameter binging map type alias. */
    typedef std::map<std::uint16_t, parameter> parameter_binding_map;

public:
    /**
     * Default constructor.
     */
    parameter_set_impl() = default;

    // Deleted
    parameter_set_impl(parameter_set_impl &&) = delete;
    parameter_set_impl(const parameter_set_impl &) = delete;
    parameter_set_impl &operator=(parameter_set_impl &&) = delete;
    parameter_set_impl &operator=(const parameter_set_impl &) = delete;

    /**
     * Set parameters set size.
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
     * Unbind all parameters.
     */
    void unbind_all();

    /**
     * Get number of bound parameters.
     *
     * @return Number of bound parameters.
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
     * Prepare parameters set for statement execution.
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
    [[nodiscard]] const parameter *get_parameter(std::uint16_t idx) const;

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
     * Write only the first row of the param set using a provided writer.
     * @param writer Writer.
     */
    void write(protocol::writer &writer) const override;

    /**
     * Write rows of the param set in the interval [begin, end) using a provided writer.
     *
     * @param writer Writer.
     * @param begin Beginning of the interval.
     * @param end End of the interval.
     * @param last Last page flag.
     */
    void write(protocol::writer &writer, SQLULEN begin, SQLULEN end, bool last) const override;

    /**
     * Calculate row length.
     *
     * @return Row length.
     */
    [[nodiscard]] std::int32_t calculate_row_len() const;

    /**
     * Get parameter set size.
     *
     * @return Number of rows in a set.
     */
    [[nodiscard]] std::int32_t get_param_set_size() const override;

    /**
     * Set the number of parameters processed in batch.
     *
     * @param processed Processed.
     */
    void set_params_processed(SQLULEN processed) override;

    /**
     * The number of processed params should be written using the provided address.
     *
     * @param ptr Pointer.
     */
    void set_params_processed_ptr(SQLULEN *ptr);

    /**
     * Get a pointer to write number of parameters processed in batch.
     *
     * @return Pointer to write number of parameters processed in batch.
     */
    [[nodiscard]] SQLULEN *get_params_processed_ptr() const;

    /**
     * Set pointer to array in which to return the status of each
     * set of parameters.
     * @param value Value.
     */
    void set_params_status_ptr(SQLUSMALLINT *value);

    /**
     * Get pointer to array in which to return the status of each
     * set of parameters.
     * @return Value.
     */
    [[nodiscard]] SQLUSMALLINT *get_params_status_ptr() const override;

    /**
     * Set parameter status.
     * @param idx parameter index.
     * @param status Status to set.
     */
    void set_params_status(int64_t idx, SQLUSMALLINT status) const;

private:
    /**
     * Write a single row of the param set using a provided writer.
     * @param writer Writer.
     * @param idx Row index.
     */
    void write_row(protocol::writer &writer, SQLULEN idx) const;

    /** Parameters. */
    parameter_binding_map m_params{};

    /** Offset added to pointers to change binding of parameters. */
    int *m_param_bind_offset{nullptr};

    /** Processed parameters. */
    SQLULEN *m_processed_param_rows{nullptr};

    /** Parameters status. */
    SQLUSMALLINT *m_params_status{nullptr};

    /** Parameter set size. */
    SQLULEN m_param_set_size{1};

    /** Current position in a parameter set. */
    SQLULEN m_param_set_pos{0};

    /** Index of the parameter, which is currently being set. */
    std::uint16_t m_current_param_idx{0};
};

} // namespace ignite
