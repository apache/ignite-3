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

#include "message_listener.h"

#include <list>

namespace ignite::proxy {

using asio::ip::tcp;

static constexpr size_t BUFF_SIZE = 4096;

struct configuration {
    asio::ip::port_type m_in_port;
    std::string m_out_host_and_port;
    std::shared_ptr<message_listener> m_listener;

    configuration(
        asio::ip::port_type m_in_port,
        const std::string &m_out_host_and_port,
        std::shared_ptr<message_listener> m_listener)
        : m_in_port(m_in_port)
        , m_out_host_and_port(m_out_host_and_port)
        , m_listener(std::move(m_listener)) { }
};

enum class direction { forward, reverse };

template<direction DIRECTION>
class session_part: public std::enable_shared_from_this<session_part<DIRECTION>> {
public:
    session_part(std::shared_ptr<tcp::socket> m_src, std::shared_ptr<tcp::socket> m_dst, std::shared_ptr<message_listener> listener)
        : m_src(std::move(m_src))
        , m_dst(std::move(m_dst))
        , m_listener(std::move(listener)) {}

    void do_read() {
        std::cout << "Reading from socket\n";

        m_src->async_read_some(asio::buffer(m_buf, BUFF_SIZE),
        [self = std::move(this->shared_from_this())](const asio::error_code& ec, size_t len) {
            if (ec) {
                if (ec == asio::error::eof) {
                    return;
                }
                throw std::runtime_error("Error while reading from socket " + ec.message());
            }

            message m{self->m_buf.begin(), self->m_buf.begin() + len};

            if (self->m_listener) {
                if constexpr (DIRECTION == direction::forward) {
                    self->m_listener->register_out_message(m);
                } else {
                    self->m_listener->register_in_message(m);
                }
            }

            self->do_write(std::move(m));
        });
    }

    void do_write(message&& msg) {
        asio::async_write(
            *m_dst, asio::buffer(msg.data(), msg.size()),
            [self = std::move(this->shared_from_this())](asio::error_code ec, size_t) {
                if (ec) {
                    if (ec == asio::error::eof) {
                        return;
                    }
                    throw std::runtime_error("Error while writing to socket " + ec.message());
                }

                self->do_read();
            });
    }

private:
    std::shared_ptr<tcp::socket> m_src;
    std::shared_ptr<tcp::socket> m_dst;
    std::array<char, BUFF_SIZE> m_buf{};
    std::shared_ptr<message_listener> m_listener{nullptr};

};

class session : public std::enable_shared_from_this<session> {
public:
    session(
        std::shared_ptr<tcp::socket> in_sock,
        std::shared_ptr<tcp::socket> out_sock,
        std::shared_ptr<message_listener> listener)
        : m_in_sock(std::move(in_sock))
        , m_out_sock(std::move(out_sock))
    {
        m_forward_part = std::make_shared<session_part<direction::forward>>(m_in_sock, m_out_sock, listener);
        m_reverse_part = std::make_shared<session_part<direction::reverse>>(m_out_sock, m_in_sock, listener);
    }

    void start() { do_serve(); }

    tcp::socket &get_out_sock() { return *m_out_sock; }

private:
    void do_serve() {
        m_forward_part->do_read();
        m_reverse_part->do_read();
    }

    std::shared_ptr<tcp::socket> m_in_sock;
    std::shared_ptr<tcp::socket> m_out_sock;

    std::shared_ptr<session_part<direction::forward>> m_forward_part;
    std::shared_ptr<session_part<direction::reverse>> m_reverse_part;
};

class asio_proxy {
public:
    asio_proxy(std::vector<configuration> configurations)
        : m_resolver(m_io_context)
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
        std::shared_ptr<message_listener> m_listener;

        proxy_entry(
            asio::io_context& io_context,
            asio::ip::port_type in_port,
            const std::string& out_host_and_port,
            std::shared_ptr<message_listener> listener)
            : m_in_acceptor(io_context, tcp::endpoint(tcp::v4(), in_port))
            , m_listener(std::move(listener))
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

        entry.m_in_acceptor.async_accept([this, &entry](asio::error_code ec, tcp::socket in_sock) {
            if (ec) {
                throw std::runtime_error("Error accepting incoming connection " + ec.message());
            }

            auto p_in_sock = std::make_shared<tcp::socket>(std::move(in_sock));
            auto p_out_sock = std::make_shared<tcp::socket>(m_io_context);
            auto ses = std::make_shared<session>(p_in_sock, p_out_sock, entry.m_listener);

            m_resolver.async_resolve(entry.m_out_host, entry.m_out_port,
                [ses](asio::error_code ec, tcp::resolver::results_type endpoints) { // NOLINT(*-unnecessary-value-param)
                    if (ec) {
                        throw std::runtime_error("Error resolving server's address " + ec.message());
                    }

                    asio::async_connect(
                        ses->get_out_sock(), endpoints, [ses](const asio::error_code &ec, const tcp::endpoint &e) {
                            if (ec) {
                                throw std::runtime_error(
                                    "Error connecting to server " + ec.message()
                                    + " port=" + std::to_string(e.port())
                                );
                            }

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

    std::atomic_bool m_stopped{false};
};
} // namespace ignite::proxy
