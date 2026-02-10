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

#include "ignite/common/ignite_error.h"
#include "ignite/common/primitive.h"

#include <initializer_list>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ignite {
// Forward declaration.
class ignite_tuple;

namespace detail {
ignite_tuple concat(const ignite_tuple &left, const ignite_tuple &right);
}

/**
 * @brief Ignite tuple.
 *
 * May represent table record or key/value part of the record.
 * Contains uniquely named fields types of which are restricted by @ref ignite::primitive.
 */
class ignite_tuple {
    friend ignite_tuple detail::concat(const ignite_tuple &left, const ignite_tuple &right);

public:
    // Default
    ignite_tuple() = default;

    /**
     * Constructor.
     *
     * @param capacity Capacity.
     */
    explicit ignite_tuple(size_t capacity) {
        m_pairs.reserve(capacity);
        m_indices.reserve(capacity);
    }

    /**
     * Constructor.
     *
     * @param pairs Pairs.
     */
    ignite_tuple(std::initializer_list<std::pair<std::string, primitive>> pairs)
        : m_pairs(pairs)
        , m_indices() {
        m_indices.reserve(pairs.size());
        for (size_t i = 0; i < m_pairs.size(); ++i)
            m_indices.emplace(std::make_pair(parse_name(m_pairs[i].first), i));
    }

    /**
     * Gets a number of columns in the tuple.
     *
     * @return Number of columns in the tuple.
     */
    [[nodiscard]] std::int32_t column_count() const noexcept { return std::int32_t(m_pairs.size()); }

    /**
     * Gets the value of the specified column.
     *
     * @param idx The column index.
     * @return Column value.
     */
    [[nodiscard]] const primitive &get(uint32_t idx) const {
        if (idx > m_pairs.size()) {
            throw ignite_error(error::code::ILLEGAL_ARGUMENT,
                "Index is too large: idx=" + std::to_string(idx) + ", columns_num=" + std::to_string(m_pairs.size()));
        }
        return m_pairs[idx].second;
    }

    /**
     * Gets the value of the specified column.
     *
     * @tparam T Column type.
     * @param idx The column index.
     * @return Column value.
     */
    template<typename T>
    [[nodiscard]] T get(uint32_t idx) const {
        return get(idx).template get<T>();
    }

    /**
     * Sets the value of the specified column.
     *
     * @tparam T Column type.
     * @param idx The column index.
     * @param value Value.
     */
    template<typename T>
    void set(uint32_t idx, T &&value) {
        if (idx > m_pairs.size()) {
            throw ignite_error(error::code::ILLEGAL_ARGUMENT,
                "Index is too large: idx=" + std::to_string(idx) + ", columns_num=" + std::to_string(m_pairs.size()));
        }
        m_pairs[idx].second = std::forward<T>(value);
    }

    /**
     * Gets the value of the specified column.
     *
     * @param name The column name.
     * @return Column value.
     */
    [[nodiscard]] const primitive &get(std::string_view name) const {
        auto it = m_indices.find(parse_name(name));
        if (it == m_indices.end())
            throw ignite_error(error::code::ILLEGAL_ARGUMENT,
                "Can not find column with the name '" + std::string(name) + "' in the tuple");
        auto idx = it->second;
        return m_pairs[idx].second;
    }

    /**
     * Gets the value of the specified column.
     *
     * @tparam T Column type.
     * @param name The column name.
     * @return Column value.
     */
    template<typename T>
    [[nodiscard]] T get(std::string_view name) const {
        return get(name).template get<T>();
    }

    /**
     * Sets the value of the specified column.
     *
     * @tparam T Column type.
     * @param name The column name.
     * @param value Value.
     */
    template<typename T>
    void set(std::string_view name, T &&value) {
        auto parsed_name = parse_name(name);
        auto it = m_indices.find(parsed_name);
        if (it != m_indices.end()) {
            auto idx = it->second;
            m_pairs[idx].second = std::forward<T>(value);
            return;
        }

        m_pairs.emplace_back(name, std::forward<T>(value));
        m_indices.emplace(parsed_name, m_pairs.size() - 1);
    }

    /**
     * Gets the name of the column, given the zero-based column ordinal.
     *
     * @param idx The column index.
     * @return Column name.
     */
    [[nodiscard]] const std::string &column_name(uint32_t idx) const {
        if (idx > m_pairs.size()) {
            throw ignite_error(error::code::ILLEGAL_ARGUMENT,
                "Index is too large: idx=" + std::to_string(idx) + ", columns_num=" + std::to_string(m_pairs.size()));
        }
        return m_pairs[idx].first;
    }

    /**
     * Gets the column ordinal given the name of the column, or -1 when
     * the column with the given name does not exist.
     *
     * @param name The column name.
     * @return Column index.
     */
    [[nodiscard]] std::int32_t column_ordinal(std::string_view name) const {
        auto it = m_indices.find(parse_name(name));
        if (it == m_indices.end())
            return -1;
        return std::int32_t(it->second);
    }

private:
    /**
     * Constructor.
     *
     * @param pairs Pairs.
     * @param indices Indices.
     */
    ignite_tuple(
        std::vector<std::pair<std::string, primitive>> &&pairs, std::unordered_map<std::string, size_t> indices)
        : m_pairs(std::move(pairs))
        , m_indices(std::move(indices)) {}

    /**
     * Normalize column name.
     *
     * @param name The column name.
     * @return Normalized column name.
     */
    [[nodiscard]] static std::string parse_name(std::string_view name) {
        if (name.size() >= 2 && name.front() == '"' && name.back() == '"')
            name = name.substr(1, name.size() - 2);

        if (name.empty())
            throw ignite_error("Column name can not be an empty string");

        std::string res;
        res.reserve(name.size());

        for (auto c : name) {
            res.push_back(char(std::toupper(c)));
        }

        return res;
    }

    /** Pairs of column names and values. */
    std::vector<std::pair<std::string, primitive>> m_pairs;

    /** Indices of the columns corresponding to their names. */
    std::unordered_map<std::string, std::size_t> m_indices;
};

} // namespace ignite
