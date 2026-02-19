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

#include <iostream>
#include <queue>
#include <tuple>

#include <asio.hpp>
#include <asio/ts/buffer.hpp>
#include <asio/ts/internet.hpp>

namespace ignite::proxy {

using asio::ip::tcp;

struct message {
    char *m_arr;
    size_t m_size;

    message(char *arr, size_t size)
        : m_arr(nullptr)
        , m_size(size) {
        m_arr = new char[m_size];
        std::memcpy(m_arr, arr, size);
    }

    ~message() { delete[] m_arr; }
};

class session : public std::enable_shared_from_this<session> {
public:
    session(tcp::socket in_sock, tcp::socket out_sock)
        : m_in_sock(std::move(in_sock))
        , m_out_sock(std::move(out_sock)) { }

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
        auto tup = get_sockets_and_queue(direction);
        tcp::socket &src = std::get<0>(tup);
        std::queue<message> &queue = std::get<2>(tup);
        bool &writable = std::get<3>(tup);

        src.async_read_some(asio::buffer(buf, BUFF_SIZE),
    [this, &queue, direction, &writable](asio::error_code ec, size_t len) {
            if (ec) {
                throw std::runtime_error("Error while reading from socket " + ec.message());
            }

            // we have one-threaded executor no synchronization is needed
            queue.emplace(buf, len);

            if (writable) { // there are pending write operation on this socket
                do_write(direction);
            }

            do_read(direction);
        });
    }

    void do_write(direction direction) {
        auto tup = get_sockets_and_queue(direction);
        tcp::socket &dst = std::get<1>(tup);
        std::queue<message> &queue = std::get<2>(tup);
        bool &writable = std::get<3>(tup);

        writable = false; // protects from writing same buffer twice (from head of queue).

        if (!queue.empty()) {
            message &msg = queue.front();

            asio::async_write(
                dst, asio::buffer(msg.m_arr, msg.m_size), [this, &queue, direction, &writable](asio::error_code ec, size_t) {
                    if (ec) {
                        throw std::runtime_error("Error while writing to socket " + ec.message());
                    }

                    queue.pop();

                    if (!queue.empty()) {
                        // makes writes on the same socket strictly ordered
                        do_write(direction);
                    } else {
                        writable = true; // now read operation can initiate writes
                    }
                });
        }
    }

    std::tuple<tcp::socket &, tcp::socket &, std::queue<message> &, bool&> get_sockets_and_queue(direction direction) {
        switch (direction) {
            case forward:
                return {m_in_sock, m_out_sock, m_in_to_out, m_in_to_out_writable};
            case reverse:
                return {m_out_sock, m_in_sock, m_out_to_in, m_out_to_in_writable};
        }

        throw std::runtime_error("Should be unreachable");
    }

    tcp::socket m_in_sock;
    tcp::socket m_out_sock;

    bool m_in_to_out_writable{false};
    bool m_out_to_in_writable{false};

    std::queue<message> m_in_to_out;
    std::queue<message> m_out_to_in;

    static constexpr size_t BUFF_SIZE = 4096;

    char buf[BUFF_SIZE];
};

class asio_proxy {
public:
    asio_proxy(asio::io_context &io_context, short port)
        : m_io_context(io_context)
        , m_acceptor(m_io_context, tcp::endpoint(tcp::v4(), port))
        , m_resolver(m_io_context)
        , m_in_sock(m_io_context) {
        do_accept();
    }

private:
    void do_accept() {
        m_acceptor.async_accept(m_in_sock, [this](asio::error_code ec) {
            if (!ec) {
                auto ses = m_sessons.emplace_back(
                    std::make_shared<session>(std::move(m_in_sock), tcp::socket{m_io_context}));

                m_resolver.async_resolve(
                    "127.0.0.1", "50900", [ses](asio::error_code ec, tcp::resolver::results_type endpoints) {
                        if (!ec) {
                            asio::async_connect(ses->get_out_sock(), endpoints,
                                [&ses](const asio::error_code &ec, const tcp::endpoint & e) {
                                    if (!ec) {
                                        ses->set_writable(true);
                                        ses->start();
                                    } else {
                                        std::cout << e.port();
                                        throw std::runtime_error("Error connecting to server " + ec.message());
                                    }
                                });
                        } else {
                            throw std::runtime_error("Error resolving server's address " + ec.message());
                        }
                    });

                do_accept();

            } else {
                throw std::runtime_error("Error accepting incoming connection " + ec.message());
            }
        });
    }

    asio::io_context &m_io_context;
    tcp::acceptor m_acceptor;
    tcp::resolver m_resolver;
    tcp::socket m_in_sock;

    std::vector<std::shared_ptr<session>> m_sessons;
};
} // namespace ignite::proxy
