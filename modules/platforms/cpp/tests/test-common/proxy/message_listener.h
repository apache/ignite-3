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

#include "ignite/network/length_prefix_codec.h"
#include "ignite/protocol/client_operation.h"
#include "ignite/protocol/reader.h"

#include <atomic>
#include <cassert>
#include <queue>
#include <shared_mutex>
#include <utility>

namespace ignite::proxy {

using raw_message = std::vector<char>;

class structured_message {
public:
    structured_message() = default;

    structured_message(const structured_message& structured_message) = default;

    explicit structured_message(std::vector<std::byte> data)
        :m_data(std::move(data)) { }

    [[nodiscard]] protocol::reader payload_reader() const {
        return protocol::reader{{m_data.data() + m_payload_pos, m_data.size() - m_payload_pos}};
    }
protected:
    std::vector<std::byte> m_data{};
    size_t m_payload_pos{};
};

/**
 * Message which clients sends to server.
 */
class client_message: public structured_message{
public:
    client_message() = default;

    client_message(const client_message& msg) = default;

    explicit client_message(std::vector<std::byte> data)
        : structured_message(std::move(data))
    {
        protocol::reader rd{{m_data.data(), m_data.size()}};

        m_op = static_cast<protocol::client_operation>(rd.read_int32());
        m_req_id = rd.read_int64();

        m_payload_pos = rd.position();

        assert(m_payload_pos <= m_data.size());
    }

    [[nodiscard]] protocol::client_operation get_op() const { return m_op; }

    [[nodiscard]] int64_t get_req_id() const { return m_req_id; }

private:
    protocol::client_operation m_op{};
    int64_t m_req_id{};
};

/**
 * Message which server sends to client.
 */
class server_message : public structured_message {
public:
    server_message() = default;

    server_message(const server_message&) = default;

    explicit server_message(std::vector<std::byte> data)
        : structured_message(std::move(data))
    {
        protocol::reader rd{{m_data.data(), m_data.size()}};

        m_req_id = rd.read_int64();
        m_flags = rd.read_int32();
        m_obs_ts = rd.read_int64();

        m_payload_pos = rd.position();

        assert(m_payload_pos <= m_data.size());
    }

    [[nodiscard]] int64_t get_req_id() const { return m_req_id; }

    [[nodiscard]] int32_t get_flags() const { return m_flags; }

    [[nodiscard]] int64_t get_obs_ts() const { return m_obs_ts; }

private:
    int64_t m_req_id{};
    int32_t m_flags{};
    int64_t m_obs_ts{};
};

/**
 * Intercepts messages which go through @c asio_proxy.
 */
class message_listener {
public:
    void register_message(raw_message msg) {
        if (enable_message_registration.load()) {
            std::unique_lock lock(m_mutex);
            m_queue.push(std::move(msg));
        }
    }

    [[nodiscard]] std::queue<raw_message> get_msg_queue() const {
        std::shared_lock lock(m_mutex);
        return m_queue;
    }

    template<typename MESSAGE_TYPE>
    std::vector<MESSAGE_TYPE> get_next() {
        std::unique_lock lock(m_mutex);

        std::vector<MESSAGE_TYPE> res;

        while (!m_queue.empty() && res.empty()) {
            auto& chunk = m_queue.front();

            network::data_buffer_ref buf{{chunk.data(), chunk.size()}};

            while (true) {
                auto out  = codec.decode(buf);

                if (out.empty()) {
                    break;
                }

                auto out_bv = out.get_bytes_view();
                std::vector<std::byte> data{out_bv.begin(), out_bv.end()};
                res.emplace_back(std::move(data));
            }
            m_queue.pop();
        }

        return res;
    }

    /**
     * Enable/disable message registration for message listeners;
     * @param enable @c True if registration enabled, otherwise disabled.
     */
    void toggle_message_registration(bool enable) {
        enable_message_registration.store(enable);
    }

private:
    std::queue<raw_message> m_queue{};

    mutable std::shared_mutex m_mutex;

    network::length_prefix_codec codec;

    std::atomic_bool enable_message_registration{true};
};
} // namespace ignite::proxy
