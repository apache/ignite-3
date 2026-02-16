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

#pragma once
#include <cassert>
#include <cstring>
#include <memory>
#include <queue>
#include <sys/epoll.h>
#include <thread>
#include <unordered_map>

namespace ignite::proxy {

struct proxy_connection;

class kgb_proxy {
public:
    static constexpr int MAX_EVENTS = 64;
    static constexpr int BUFF_SIZE = 4096;

    kgb_proxy(int in_port, int out_port)
        : in_port(in_port)
        , out_port(out_port) { }

    ~kgb_proxy();
    void start();

private:
    void enable_writable_notification(int fd);
    void disable_writable_notification(int fd);
    void do_serve();
    void add_event_fd();
    void fire_stop_event();
    void start_server_socket();
    void add_socket_to_epoll(int fd);
    void process_incoming_connection();
    void process_socket_event(const epoll_event &ep_ev);
    const int in_port;
    const int out_port;
    int server_fd{-1};
    int epoll_fd{-1};
    int stop_event_fd{-1};
    std::unique_ptr<std::thread> m_polling_thread{};
    bool connected{false};
    std::unordered_map<int, std::shared_ptr<proxy_connection>> connections{};
};

struct message_chunk {
    char* m_msg = nullptr;
    size_t m_size;

    message_chunk(char *msg, size_t size)
        : m_size(size)
    {
        assert(size <= kgb_proxy::BUFF_SIZE);

        m_msg = new char[size];

        std::memcpy(m_msg, msg, size);
    }

    ~message_chunk() {
        delete[] m_msg;
    }
};


struct proxy_connection {
    int in_sock;
    int out_sock;

    proxy_connection(int cl_sock, int srv_sock)
        : in_sock(cl_sock)
        , out_sock(srv_sock) {}

    std::queue<message_chunk> in2out_queue{};
    std::queue<message_chunk> out2in_queue{};
};

} // namespace ignite::proxy