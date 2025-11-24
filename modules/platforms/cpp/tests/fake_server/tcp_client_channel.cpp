//
// Created by Ed on 25.11.2025.
//

#include "tcp_client_channel.h"
#include <arpa/inet.h>

std::vector<std::byte> ignite::tcp_client_channel::read_next_n_bytes(size_t n) {
    std::vector<std::byte> res;
    res.reserve(n);

    while (res.size() < n) {
        if (m_remaining == 0) {
            receive_next_packet();
        }

        size_t bytes_to_consume = std::min(m_remaining, n);

        std::copy_n(m_buf + m_pos, bytes_to_consume, std::back_inserter(res));

        m_pos += bytes_to_consume;
        m_remaining -= bytes_to_consume;
    }

    return res;
}

void ignite::tcp_client_channel::receive_next_packet() {
    int received = ::recv(m_cl_fd, m_buf, sizeof(m_buf), 0);

    if (received <= 0) {
        throw ignite_error("connection was closed by client");
    }

    m_remaining = received;
    m_pos = 0;
}