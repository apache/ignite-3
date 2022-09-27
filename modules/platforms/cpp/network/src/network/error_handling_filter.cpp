/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "network/error_handling_filter.h"

namespace ignite::network
{

void ErrorHandlingFilter::onConnectionSuccess(const EndPoint &addr, uint64_t id)
{
    closeConnectionOnException(id, [this, &addr, id] { DataFilterAdapter::onConnectionSuccess(addr, id); });
}

void ErrorHandlingFilter::onConnectionError(const EndPoint &addr, IgniteError err)
{
    try
    {
        DataFilterAdapter::onConnectionError(addr, std::move(err));
    }
    catch (...)
    {
        // No-op.
    }
}

void ErrorHandlingFilter::onConnectionClosed(uint64_t id, std::optional<IgniteError> err)
{
    try
    {
        DataFilterAdapter::onConnectionClosed(id, std::move(err));
    }
    catch (...)
    {
        // No-op.
    }
}

void ErrorHandlingFilter::onMessageReceived(uint64_t id, const DataBufferRef& data)
{
    closeConnectionOnException(id, [this, id, &data] { DataFilterAdapter::onMessageReceived(id, data); });
}

void ErrorHandlingFilter::onMessageSent(uint64_t id)
{
    closeConnectionOnException(id, [this, id] { DataFilterAdapter::onMessageSent(id); });
}

void ErrorHandlingFilter::closeConnectionOnException(uint64_t id, const std::function<void()>& func)
{
    try
    {
        func();
    }
    catch (const IgniteError& err)
    {
        DataFilterAdapter::close(id, err);
    }
    catch (std::exception& err)
    {
        std::string msg("Standard library exception is thrown: ");
        msg += err.what();
        IgniteError err0(StatusCode::GENERIC, msg);
        DataFilterAdapter::close(id, std::move(err0));
    }
    catch (...)
    {
        IgniteError err0(StatusCode::UNKNOWN,
            "Unknown error is encountered when processing network event");
        DataFilterAdapter::close(id, std::move(err0));
    }
}

}
