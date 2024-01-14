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

void error_handling_filter::on_connection_success(const end_point &addr, uint64_t id) {
    close_connection_on_exception(id, [this, &addr, id] { data_filter_adapter::on_connection_success(addr, id); });
}

void error_handling_filter::on_connection_error(const end_point &addr, ignite_error err) {
    try {
        data_filter_adapter::on_connection_error(addr, std::move(err));
    } catch (...) {
        // No-op.
    }
}

void error_handling_filter::on_connection_closed(uint64_t id, std::optional<ignite_error> err) {
    try {
        data_filter_adapter::on_connection_closed(id, std::move(err));
    } catch (...) {
        // No-op.
    }
}

void error_handling_filter::on_message_received(uint64_t id, bytes_view data) {
    close_connection_on_exception(id, [this, id, &data] { data_filter_adapter::on_message_received(id, data); });
}

void error_handling_filter::on_message_sent(uint64_t id) {
    close_connection_on_exception(id, [this, id] { data_filter_adapter::on_message_sent(id); });
}

void error_handling_filter::close_connection_on_exception(uint64_t id, const std::function<void()> &func) {
    try {
        func();
    } catch (const ignite_error &err) {
        data_filter_adapter::close(id, err);
    } catch (std::exception &err) {
        std::string msg("Standard library exception is thrown: ");
        msg += err.what();
        ignite_error err0(error::code::GENERIC, msg);
        data_filter_adapter::close(id, std::move(err0));
    } catch (...) {
        ignite_error err0(error::code::INTERNAL, "Unknown error is encountered when processing network event");
        data_filter_adapter::close(id, std::move(err0));
    }
}

} // namespace ignite::network
