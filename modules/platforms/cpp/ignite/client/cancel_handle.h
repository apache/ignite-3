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

/**
 * @file
 * Declares ignite::cancel_handle.
 */

#pragma once

#include "ignite/client/cancellation_token.h"
#include "ignite/common/ignite_result.h"
#include "ignite/common/detail/config.h"

#include <memory>

namespace ignite
{

/**
 * @brief Execution cancellation handle.
 *
 * A handle which may be used to request the cancellation of execution.
 */
class cancel_handle
{
public:
    /**
     * Destructor.
     */
    virtual ~cancel_handle() = default;

    /**
     * A factory method to create a handle.
     * @return A new cancel handle.
     */
    [[nodiscard]] IGNITE_API static std::shared_ptr<cancel_handle> create();

    /**
     * Abruptly terminates an execution of an associated process.
     *
     * @param callback A callback that will be called after the process has been terminated and the resources associated
     *                 with that process have been freed.
     */
    IGNITE_API virtual void cancel_async(ignite_callback<void> callback) = 0;

    /**
     * Abruptly terminates an execution of an associated process.
     *
     * Control flow will return after the process has been terminated and the resources associated with that process
     * have been freed.
     */
    IGNITE_API virtual void cancel() {
        return sync<void>([this](auto callback) mutable {
            cancel_async(std::move(callback));
        });
    }

    /**
     * Flag indicating whether cancellation was requested or not.
     *
     * This method will return true even if cancellation has not been completed yet.
     *
     * @return @c true if the cancellation was requested.
     */
    IGNITE_API virtual bool is_cancelled() const = 0;

    /**
     * Issue a token associated with this handle.
     *
     * Token is reusable, meaning the same token may be used to link several executions into a single cancellable.
     *
     * @return A token associated with this handle.
     */
    IGNITE_API virtual std::shared_ptr<cancellation_token> get_token() = 0;
};

} // namespace ignite
