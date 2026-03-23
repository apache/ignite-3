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

#include "ignite/client/cancel_handle.h"
#include "ignite/client/detail/cancellation_token_impl.h"

namespace ignite
{

/**
 * @brief Implementation for cancellation handle.
 *
 * Implementation of @ref ignite::cancel_handle.
 */
class cancel_handle_impl : public cancel_handle
{
public:
    /**
     * Constructor.
     */
    cancel_handle_impl()
        : m_token(std::make_shared<cancellation_token_impl>()) { }

    /**
     * Destructor.
     */
    ~cancel_handle_impl() override = default;

    /**
     * Abruptly terminates an execution of an associated process.
     *
     * @param callback A callback that will be called after the process has been terminated and the resources associated
     *                 with that process have been freed.
     */
    IGNITE_API void cancel_async(ignite_callback<void> callback) override {
        m_token->cancel_async(std::move(callback));
    }

    /**
     * Flag indicating whether cancellation was requested or not.
     *
     * This method will return true even if cancellation has not been completed yet.
     *
     * @return @c true if the cancellation was requested.
     */
    IGNITE_API bool is_cancelled() const override { return m_token->is_cancelled(); }

    /**
     * Issue a token associated with this handle.
     *
     * Token is reusable, meaning the same token may be used to link several executions into a single cancellable.
     *
     * @return A token associated with this handle.
     */
    IGNITE_API std::shared_ptr<cancellation_token> get_token() override {
        return m_token;
    }

private:
    /** Cancellation token. */
    std::shared_ptr<cancellation_token_impl> m_token;
};


std::shared_ptr<cancel_handle> cancel_handle::create() {
    return std::make_shared<cancel_handle_impl>();
}

} // namespace ignite
