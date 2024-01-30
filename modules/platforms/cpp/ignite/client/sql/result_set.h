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

#include "ignite/client/sql/result_set_metadata.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"

#include <functional>
#include <memory>

namespace ignite {

namespace detail {
class result_set_impl;
}

/**
 * Query result set.
 */
class result_set {
public:
    // Default
    result_set() = default;

    /**
     * Constructor
     *
     * @param impl Implementation
     */
    explicit result_set(std::shared_ptr<detail::result_set_impl> impl)
        : m_impl(std::move(impl)) {}

    /**
     * Gets metadata.
     *
     * @return Metadata.
     */
    [[nodiscard]] IGNITE_API const result_set_metadata &metadata() const;

    /**
     * Gets a value indicating whether this result set contains a collection of rows.
     *
     * @return A value indicating whether this result set contains a collection of rows.
     */
    [[nodiscard]] IGNITE_API bool has_rowset() const;

    /**
     * Gets the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.), or 0 if
     * the statement returns nothing (such as "ALTER TABLE", etc), or -1 if not applicable.
     *
     * @return The number of rows affected by the DML statement execution.
     */
    [[nodiscard]] IGNITE_API std::int64_t affected_rows() const;

    /**
     * Gets a value indicating whether a conditional query (such as "CREATE TABLE IF NOT EXISTS") was applied
     * successfully.
     *
     * @return A value indicating whether a conditional query was applied successfully.
     */
    [[nodiscard]] IGNITE_API bool was_applied() const;

    /**
     * Close result set asynchronously.
     *
     * @param callback Callback to call on completion.
     * @return @c true if the request was sent, and false if the result set was already closed.
     */
    IGNITE_API bool close_async(std::function<void(ignite_result<void>)> callback);

    /**
     * Close result set synchronously.
     *
     * @return @c true if the request was sent, and false if the result set was already closed.
     */
    IGNITE_API bool close();

    /**
     * Retrieves current page.
     * Result set is left empty after this operation and will return empty page on subsequent request
     * unless there are more available pages and you call @c fetch_next_page().
     *
     * @return Current page.
     */
    [[nodiscard]] IGNITE_API std::vector<ignite_tuple> current_page() &&;

    /**
     * Gets current page.
     *
     * @return Current page.
     */
    [[nodiscard]] IGNITE_API const std::vector<ignite_tuple> &current_page() const &;

    /**
     * Checks whether there are more pages of results.
     *
     * @return @c true if there are more pages with results and @c false otherwise.
     */
    [[nodiscard]] IGNITE_API bool has_more_pages();

    /**
     * Fetch the next page of results asynchronously.
     * The current page is changed after the operation is complete.
     *
     * @param callback Callback to call on completion.
     */
    IGNITE_API void fetch_next_page_async(std::function<void(ignite_result<void>)> callback);

    /**
     * Fetch the next page of results synchronously.
     * The current page is changed after the operation is complete.
     */
    IGNITE_API void fetch_next_page() {
        return sync<void>([this](auto callback) mutable { fetch_next_page_async(std::move(callback)); });
    }

private:
    /** Implementation. */
    std::shared_ptr<detail::result_set_impl> m_impl;
};

} // namespace ignite
