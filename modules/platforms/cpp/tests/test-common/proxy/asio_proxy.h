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
#include "gtest_logger.h"

#include <gtest/gtest.h>

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

#include <complex>
#include <list>

namespace ignite::proxy {

using asio::ip::tcp;

static constexpr size_t BUFF_SIZE = 4096;

struct configuration {
    asio::ip::port_type m_in_port;
    std::string m_out_host_and_port;
    std::shared_ptr<message_listener> m_in_listener;
    std::shared_ptr<message_listener> m_out_listener;

    configuration(
        asio::ip::port_type m_in_port,
        const std::string &m_out_host_and_port,
        std::shared_ptr<message_listener> in_listener,
        std::shared_ptr<message_listener> out_listener)
        : m_in_port(m_in_port)
        , m_out_host_and_port(m_out_host_and_port)
        , m_in_listener(std::move(in_listener))
        , m_out_listener(std::move(out_listener)) {}
};

struct proxy_entry {
    tcp::acceptor m_in_acceptor;
    std::string m_out_host;
    std::string m_out_port;
    std::shared_ptr<message_listener> m_in_listener;
    std::shared_ptr<message_listener> m_out_listener;

    proxy_entry(asio::io_context& io_context,const configuration& cfg)
        : m_in_acceptor(io_context, tcp::endpoint(tcp::v4(), cfg.m_in_port))
        , m_in_listener(std::move(cfg.m_in_listener))
        , m_out_listener(std::move(cfg.m_out_listener))
    {
        auto colon_pos = cfg.m_out_host_and_port.find(':');

        if (colon_pos == std::string::npos) {
            throw std::runtime_error("Incorrect host and part format. Expected 'hostname:port' but got " + cfg.m_out_host_and_port);
        }

        m_out_host = cfg.m_out_host_and_port.substr(0, colon_pos);
        m_out_port = cfg.m_out_host_and_port.substr(colon_pos + 1);
    }
};

class session_part: public std::enable_shared_from_this<session_part> {
public:
    session_part(
        std::shared_ptr<tcp::socket> src,
        std::shared_ptr<tcp::socket> dst,
        std::shared_ptr<message_listener> listener,
        std::atomic_bool& failed,
        std::shared_ptr<gtest_logger> logger)
        : m_src(std::move(src))
        , m_dst(std::move(dst))
        , m_listener(std::move(listener))
        , m_failed(failed)
        , m_logger(std::move(logger)) {}

    void do_read() {
        m_src->async_read_some(asio::buffer(m_buf, BUFF_SIZE),
        [self = this->shared_from_this()](const asio::error_code& ec, size_t len) {
            if (ec) {
                if (ec == asio::error::eof) {
                    return;
                }
                self->m_logger->log_error("Error while reading from socket " + ec.message());

                self->m_failed.store(true);
            }

            message m{self->m_buf.begin(), self->m_buf.begin() + len};

            if (self->m_listener) {
                self->m_listener->register_message(m);
            }

            self->do_write(std::move(m));
        });
    }

    void do_write(message&& msg) {
        asio::async_write(
            *m_dst, asio::buffer(msg.data(), msg.size()),
            [self = shared_from_this()](asio::error_code ec, size_t) {
                if (ec) {
                    if (ec == asio::error::eof) {
                        return;
                    }
                    self->m_logger->log_error("Error while writing to socket " + ec.message());

                    self->m_failed.store(true);
                }

                self->do_read();
            });
    }

private:
    std::shared_ptr<tcp::socket> m_src;
    std::shared_ptr<tcp::socket> m_dst;
    std::array<char, BUFF_SIZE> m_buf{};
    std::shared_ptr<message_listener> m_listener{nullptr};
    std::atomic_bool& m_failed;
    std::shared_ptr<gtest_logger> m_logger;
};

class session : public std::enable_shared_from_this<session> {
public:
    session(
        std::shared_ptr<tcp::socket> in_sock,
        std::shared_ptr<tcp::socket> out_sock,
        std::shared_ptr<message_listener> in_listener,
        std::shared_ptr<message_listener> out_listener,
        std::atomic_bool& failed,
        std::shared_ptr<gtest_logger> logger)
        : m_in_sock(std::move(in_sock))
        , m_out_sock(std::move(out_sock))
    {
        m_forward_part = std::make_shared<session_part>(m_in_sock, m_out_sock, in_listener, failed, logger);
        m_reverse_part = std::make_shared<session_part>(m_out_sock, m_in_sock, out_listener, failed, logger);
    }

    void connect(const tcp::resolver::results_type& endpoints) {
        asio::async_connect(*m_out_sock, endpoints,
            [self=shared_from_this()](const asio::error_code &ec, const tcp::endpoint &e) {
                if (ec) {
                    throw std::runtime_error(
                        "Error connecting to server " + ec.message()
                        + " port=" + std::to_string(e.port())
                    );
                }

                self->do_serve();
            });
    }
private:
    void do_serve() {
        m_forward_part->do_read();
        m_reverse_part->do_read();
    }

    std::shared_ptr<tcp::socket> m_in_sock;
    std::shared_ptr<tcp::socket> m_out_sock;

    std::shared_ptr<session_part> m_forward_part;
    std::shared_ptr<session_part> m_reverse_part;
};

class asio_proxy {
public:
    asio_proxy(std::vector<configuration> configurations, std::shared_ptr<gtest_logger> logger)
        : m_resolver(m_io_context)
        , m_logger(std::move(std::move(logger)))
    {
        for (auto &cfg : configurations) {
            m_conn_map.emplace(
                cfg.m_in_port,
                proxy_entry{m_io_context, cfg}
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

        if (m_failed.load()) {
            ADD_FAILURE() << "Proxy error occurred during test execution";
        }
    }

private:
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
            auto ses = std::make_shared<session>(
                p_in_sock,
                p_out_sock,
                entry.m_in_listener,
                entry.m_out_listener,
                this->m_failed,
                m_logger
            );

            tcp::resolver &resolver = m_resolver;
            resolver.async_resolve(entry.m_out_host, entry.m_out_port,
                [ses](
                    asio::error_code ec, tcp::resolver::results_type endpoints) { // NOLINT(*-unnecessary-value-param)
                    if (ec) {
                        throw std::runtime_error("Error resolving server's address " + ec.message());
                    }

                    ses->connect(endpoints);
                });

            do_accept(entry);
        });
    }

    std::map<asio::ip::port_type, proxy_entry> m_conn_map{};

    asio::io_context m_io_context{};
    std::unique_ptr<std::thread> m_executor{};

    tcp::resolver m_resolver;

    std::shared_ptr<gtest_logger> m_logger;

    std::atomic_bool m_stopped{false};

    std::atomic_bool m_failed{false};
};
} // namespace ignite::proxy
