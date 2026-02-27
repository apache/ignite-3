// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <asio/ts/internet.hpp>

#include "message.h"
#include "message_listener.h"

namespace ignite::proxy {

using asio::ip::tcp;

struct configuration {
    asio::ip::port_type m_in_port;
    std::string m_out_host_and_port;
    message_listener* m_listener;

    configuration(asio::ip::port_type m_in_port, const std::string &m_out_host_and_port, message_listener *m_listener)
        : m_in_port(m_in_port)
        , m_out_host_and_port(m_out_host_and_port)
        , m_listener(m_listener) { }
};

class session : public std::enable_shared_from_this<session> {
public:
    session(tcp::socket in_sock, tcp::socket out_sock, std::atomic_bool& stopped, message_listener* listener)
        : m_in_sock(std::move(in_sock))
        , m_out_sock(std::move(out_sock))
        , m_stopped(stopped)
        , m_listener(listener)
    { }

    ~session() {
        std::cout << "Session destructed " << this <<  std::endl;
    }

    void start() { do_serve(); }

    tcp::socket &get_out_sock() { return m_out_sock; }

    void set_writable(bool writable) {
        m_in_to_out_writable = writable;
        m_out_to_in_writable = writable;
    }

    enum direction { forward, reverse };

private:
    void do_serve() {
        do_read(forward);
        do_read(reverse);
    }

    void do_read(direction direction) {
        if (m_stopped.load())
            return;

        tcp::socket &src = direction == forward ? m_in_sock : m_out_sock;
        std::queue<message> &queue = direction == forward ? m_in_to_out : m_out_to_in;
        bool &writable = direction == forward ? m_in_to_out_writable : m_out_to_in_writable;

        auto self(shared_from_this());

        src.async_read_some(asio::buffer(buf, BUFF_SIZE),
    [&queue, direction, &writable, self](const asio::error_code& ec, size_t len) {
            if (ec) {
                if (ec == asio::error::eof) {
                    return;
                }
                throw std::runtime_error("Error while reading from socket " + ec.message());
            }

            // we have one-threaded executor no synchronization is needed
            message& msg = queue.emplace(self->buf, len);

            if (self->m_listener) {
                if (direction == forward) {
                    self->m_listener->register_out_message(msg);
                } else {
                    self->m_listener->register_in_message(msg);
                }
            }

            if (writable) { // there are pending write operation on this socket
                self->do_write(direction);
            }

            self->do_read(direction);
        });
    }

    void do_write(direction direction) {
        tcp::socket &dst = direction == forward ? m_out_sock : m_in_sock;
        std::queue<message> &queue = direction == forward ? m_in_to_out : m_out_to_in;
        bool &writable = direction == forward ? m_in_to_out_writable : m_out_to_in_writable;

        writable = false; // protects from writing same buffer twice (from head of queue).

        auto self(shared_from_this());
        if (!queue.empty()) {
            message &msg = queue.front();

            asio::async_write(
                dst, asio::buffer(msg.m_arr, msg.m_size),
                [&queue, direction, &writable, self](asio::error_code ec, size_t) {
                    if (ec) {
                        if (ec == asio::error::eof) {
                            return;
                        }
                        throw std::runtime_error("Error while writing to socket " + ec.message());
                    }

                    queue.pop();

                    if (!queue.empty()) {
                        // makes writes on the same socket strictly ordered
                        self->do_write(direction);
                    } else {
                        writable = true; // now read operation can initiate writes
                    }
                });
        }
    }

    tcp::socket m_in_sock;
    tcp::socket m_out_sock;

    bool m_in_to_out_writable{false};
    bool m_out_to_in_writable{false};

    std::queue<message> m_in_to_out;
    std::queue<message> m_out_to_in;

    static constexpr size_t BUFF_SIZE = 4096;

    char buf[BUFF_SIZE]{};

    std::atomic_bool& m_stopped;

    message_listener* m_listener{nullptr};
};

class asio_proxy {
public:
    asio_proxy(std::vector<configuration> configurations)
        : m_resolver(m_io_context)
        , m_in_sock(m_io_context)
    {
        for (auto &cfg : configurations) {
            m_conn_map.emplace(
                cfg.m_in_port,
                proxy_entry{m_io_context, cfg.m_in_port, cfg.m_out_host_and_port, cfg.m_listener}
            );
        }

        do_serve();

        m_executor = std::make_unique<std::thread>([this]() {
            m_io_context.run();
        });
    }

    ~asio_proxy() {
        m_stopped.store(true);
        m_io_context.stop();

        m_executor->join();
    }

private:
    struct proxy_entry {
        tcp::acceptor m_in_acceptor;
        std::string m_out_host;
        std::string m_out_port;
        message_listener* m_listener;

        proxy_entry(asio::io_context& io_context, asio::ip::port_type in_port, const std::string& out_host_and_port, message_listener* listener)
            : m_in_acceptor(io_context, tcp::endpoint(tcp::v4(), in_port))
            , m_listener(listener)
        {
            auto colon_pos = out_host_and_port.find(':');

            if (colon_pos == std::string::npos) {
                throw std::runtime_error("Incorrect host and part format. Expected 'hostname:port' but got " + out_host_and_port);
            }

            m_out_host = out_host_and_port.substr(0, colon_pos);
            m_out_port = out_host_and_port.substr(colon_pos + 1);
        }
    };


    void do_serve() {
        for (auto& [_, entry]: m_conn_map) {
            do_accept(entry);
        }
    }

    void do_accept(proxy_entry& entry) {
        if (m_stopped.load()) {
            return;
        }

        entry.m_in_acceptor.async_accept(m_in_sock, [this, &entry](asio::error_code ec) {
            if (ec) {
                throw std::runtime_error("Error accepting incoming connection " + ec.message());
            }

            auto ses = std::make_shared<session>(std::move(m_in_sock), tcp::socket{m_io_context}, m_stopped);

            m_resolver.async_resolve(entry.m_out_host, entry.m_out_port,
                [ses](asio::error_code ec, tcp::resolver::results_type endpoints) { // NOLINT(*-unnecessary-value-param)
                    if (ec) {
                        throw std::runtime_error("Error resolving server's address " + ec.message());
                    }

                    asio::async_connect(
                        ses->get_out_sock(), endpoints, [ses](const asio::error_code &ec, const tcp::endpoint &e) {
                            if (ec) {
                                std::cout << e.port();
                                throw std::runtime_error("Error connecting to server " + ec.message());
                            }

                            ses->set_writable(true);
                            ses->start();
                        });
                });

            do_accept(entry);
        });
    }

    std::map<asio::ip::port_type, proxy_entry> m_conn_map{};

    asio::io_context m_io_context{};
    std::unique_ptr<std::thread> m_executor{};

    tcp::resolver m_resolver;
    tcp::socket m_in_sock;

    std::atomic_bool m_stopped{false};
};
} // namespace ignite::proxy
