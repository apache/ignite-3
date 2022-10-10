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

#include "error_handling_filter.h"

namespace ignite::network {

void ErrorHandlingFilter::on_connection_success(const EndPoint &addr, uint64_t id) {
    closeConnectionOnException(id, [this, &addr, id] { DataFilterAdapter::on_connection_success(addr, id); });
}

void ErrorHandlingFilter::on_connection_error(const EndPoint &addr, ignite_error err) {
    try {
        DataFilterAdapter::on_connection_error(addr, std::move(err));
    } catch (...) {
        // No-op.
    }
}

void ErrorHandlingFilter::on_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    try {
        DataFilterAdapter::on_connection_closed(id, std::move(err));
    } catch (...) {
        // No-op.
    }
}

void ErrorHandlingFilter::on_message_received(uint64_t id, bytes_view data) {
    closeConnectionOnException(id, [this, id, &data] { DataFilterAdapter::on_message_received(id, data); });
}

void ErrorHandlingFilter::on_message_sent(uint64_t id) {
    closeConnectionOnException(id, [this, id] { DataFilterAdapter::on_message_sent(id); });
}

void ErrorHandlingFilter::closeConnectionOnException(uint64_t id, const std::function<void()> &func) {
    try {
        func();
    } catch (const ignite_error &err) {
        DataFilterAdapter::close(id, err);
    } catch (std::exception &err) {
        std::string msg("Standard library exception is thrown: ");
        msg += err.what();
        ignite_error err0(status_code::GENERIC, msg);
        DataFilterAdapter::close(id, std::move(err0));
    } catch (...) {
        ignite_error err0(status_code::UNKNOWN, "Unknown error is encountered when processing network event");
        DataFilterAdapter::close(id, std::move(err0));
    }
}

} // namespace ignite::network
