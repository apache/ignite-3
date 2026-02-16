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

//

#include "kgb_proxy.h"

#include <ignite/network/detail/linux/sockets.h>

#include <fcntl.h>
#include <iostream>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ignite::proxy {

void set_socket_non_blocking(int fd) {
    using network::detail::set_non_blocking_mode;

    if (!set_non_blocking_mode(fd, true)) {
        throw std::runtime_error("Error making socket non-blocking");
    }
}

void kgb_proxy::enable_writable_notification(int fd) { // NOLINT(*-make-member-function-const)
    epoll_event ep_ev{};

    ep_ev.data.fd = fd;
    ep_ev.events = EPOLLIN | EPOLLOUT | EPOLLET;

    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ep_ev);
}

void kgb_proxy::disable_writable_notification(int fd) { // NOLINT(*-make-member-function-const)
    epoll_event ep_ev{};

    ep_ev.data.fd = fd;
    ep_ev.events = EPOLLIN | EPOLLET;

    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ep_ev);
}

void kgb_proxy::do_serve() {
    epoll_event events[MAX_EVENTS];

    bool stopped = false;
    while (!stopped) {
        int event_cnt = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

        for (int i = 0; i < event_cnt; ++i) {
            int fd = events[i].data.fd;

            if (fd == stop_event_fd) {
                uint64_t val;
                read(stop_event_fd, &val, sizeof(val));
                stopped = true;
            } else if (fd == server_fd) {
                process_incoming_connection();
            } else {
                process_socket_event(events[i]);
            }
        }
    }
}

void kgb_proxy::add_event_fd() {
    stop_event_fd = eventfd(0, EFD_NONBLOCK);

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = stop_event_fd;

    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, stop_event_fd, &ev);
}

void kgb_proxy::fire_stop_event() { // NOLINT(*-make-member-function-const)
    uint64_t one = 1;
    write(stop_event_fd, &one, sizeof(one));
}

void kgb_proxy::start_server_socket() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    set_socket_non_blocking(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};

    addr.sin_family = AF_INET;
    addr.sin_port = htons(in_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server_fd, (sockaddr *) &addr, sizeof(addr));
    listen(server_fd, 16);

    epoll_event ev{};

    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
}

void kgb_proxy::add_socket_to_epoll(int fd) { // NOLINT(*-make-member-function-const)
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = fd;

    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

void kgb_proxy::process_incoming_connection() {
    while (true) {
        int in_fd;
        {
            sockaddr_in addr{};
            socklen_t len = sizeof(addr);

            in_fd = accept(server_fd, (sockaddr*) &addr, &len);

            if (in_fd < 0) {
                if (errno != EAGAIN) {
                    std::error_code ec{errno, std::system_category()};
                    std::cerr << "Unexpected issue when accepting connection err=" << ec.message() << "\n";
                }
                break;
            }

            set_socket_non_blocking(in_fd);

            std::cout << "Client connected to proxy fd = " << in_fd << std::endl;
        }

        int out_fd;
        {
            out_fd = socket(AF_INET, SOCK_STREAM, 0);

            set_socket_non_blocking(out_fd);

            if (out_fd < 0) {
                std::error_code ec{errno, std::system_category()};
                throw std::runtime_error("Unable to create socket for outbound proxy connection err = " + ec.message());
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(out_port);

            inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

            int res = connect(out_fd, (sockaddr*) &addr, sizeof(addr));

            if (res < 0) {
                if (errno != EINPROGRESS) {
                    std::error_code ec{errno, std::system_category()};
                    throw std::runtime_error("Unable to connection to server err = " + ec.message());
                }
            }

            std::cout << "Proxy connected to server fd = " << out_fd << std::endl;
        }

        std::shared_ptr<proxy_connection> conn = std::make_shared<proxy_connection>(in_fd, out_fd);

        connections[in_fd] = conn;
        connections[out_fd] = conn;

        add_socket_to_epoll(in_fd);
        add_socket_to_epoll(out_fd);

        std::cout << "Socket pair has been created in_fd = " << in_fd << " out_fd = " << out_fd << std::endl;
    }
}

void kgb_proxy::process_socket_event(const epoll_event& ep_ev) {
    int fd = ep_ev.data.fd;

    auto it = connections.find(fd);
    if (it == connections.end()) {
        throw std::runtime_error("Event for unknown socket occurred fd = " + std::to_string(fd));
    }

    auto conn = it->second;

    if (ep_ev.events & EPOLLIN) {

        char buf[BUFF_SIZE];

        int src = fd;
        int dst = src == conn->in_sock ? conn->out_sock : conn->in_sock;

        while (true) {
            ssize_t received = recv(src, buf, sizeof(buf), 0);

            if (received < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }

                perror("recv");
                break;

            }

            if (received == 0) {
                close(src);
                close(dst);

                connections.erase(src);
                connections.erase(dst);
                break;
            }

            auto& queue = src == conn->in_sock ? conn->in2out_queue : conn->out2in_queue;
            queue.emplace(buf, received);

            enable_writable_notification(dst);
        }
    }

    if (ep_ev.events & EPOLLOUT) {
        int dst = fd;
        int src = dst == conn->in_sock ? conn->out_sock : conn->in_sock;

        auto& queue = src == conn->in_sock ? conn->in2out_queue : conn->out2in_queue;

        while (!queue.empty()) {

            const message_chunk& chunk = queue.front();
            ssize_t sent = send(dst, chunk.m_msg, chunk.m_size, 0);

            if (sent <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }

                perror("send");
                break;
            }

            queue.pop();
        }

        disable_writable_notification(dst);
    }
}

kgb_proxy::~kgb_proxy() {
    fire_stop_event();

    m_polling_thread->join();

    close(stop_event_fd);
    close(server_fd);
    close(epoll_fd);
}

void kgb_proxy::start() {
    epoll_fd = epoll_create1(0);

    add_event_fd();

    start_server_socket();

    std::cout << "Server listening on " << in_port << std::endl;

    m_polling_thread = std::make_unique<std::thread>([this]() {
        do_serve();
    });
}
} // namespace ignite::proxy