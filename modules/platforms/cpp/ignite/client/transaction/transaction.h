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

#include "ignite/common/config.h"
#include "ignite/common/ignite_result.h"

namespace ignite {

/**
 * Ignite tuple.
 */
class transaction {
public:
    // Default
    transaction() = default;

    /**
     * Commits the transaction.
     */
    IGNITE_API void commit() {
        return sync<void>([this](auto callback) { commit_async(std::move(callback)); });
    }

    /**
     * Commits the transaction asynchronously.
     */
    IGNITE_API void commit_async(ignite_callback<void> on_complete) {
        (void) on_complete;
        throw ignite_error("Transactions are not yet supported");
    }

    /**
     * Rollbacks the transaction.
     */
    IGNITE_API void rollback() {
        return sync<void>([this](auto callback) { rollback_async(std::move(callback)); });
    }

    /**
     * Rollbacks the transaction asynchronously.
     */
    IGNITE_API void rollback_async(ignite_callback<void> on_complete) {
        (void) on_complete;
        throw ignite_error("Transactions are not yet supported");
    }

private:
};

} // namespace ignite
