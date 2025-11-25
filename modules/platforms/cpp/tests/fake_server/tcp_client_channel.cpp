//
// Created by Ed on 25.11.2025.
//

#include "tcp_client_channel.h"
#include <arpa/inet.h>

std::vector<std::byte> ignite::tcp_client_channel::read_next_n_bytes(size_t n) {
    std::vector<std::byte> res;
    res.reserve(n);

    while (res.size() < n && !m_stopped) {
        if (m_remaining == 0) {
            receive_next_packet();
        }

        size_t bytes_to_consume = std::min(m_remaining, n);

        std::copy_n(m_buf + m_pos, bytes_to_consume, std::back_inserter(res));

        m_pos += bytes_to_consume;
        m_remaining -= bytes_to_consume;
    }

    if (m_stopped) {
        return {};
    }

    return res;
}

void ignite::tcp_client_channel::send_message(std::vector<std::byte> msg) {
    ::send(m_cl_fd, msg.data(), msg.size(), 0);
}

void ignite::tcp_client_channel::receive_next_packet() {
    int received = ::recv(m_cl_fd, m_buf, sizeof(m_buf), 0);

    if (received == 0) {
        std::cout << "connection was closed" << std::endl;

        m_stopped.store(true);
    }

    if (received < 0 && !m_stopped) {
        std::stringstream ss;

        ss << "connection was closed with error: " << strerror(errno);
        throw ignite_error(ss.str());
    }

    m_remaining = received;
    m_pos = 0;
}

void ignite::tcp_client_channel::start() {
    sockaddr_in cl_addr{};

    socklen_t addr_len = sizeof(cl_addr);

    std::cout << "waiting for client to connect srv_fd = " << m_srv_fd << std::endl;

    m_cl_fd = accept(m_srv_fd, reinterpret_cast<sockaddr *>(&cl_addr), &addr_len);

    if (m_cl_fd < 0 && !m_stopped) {
        std::stringstream ss;
        ss << "connection acceptance failed " << strerror(errno);
        throw std::runtime_error(ss.str());
    }

    std::cout << "Client connected\n";
}

void ignite::tcp_client_channel::stop() {
    m_stopped.store(true);
    if (m_cl_fd > 0) {
        ::close(m_cl_fd);
    }
}