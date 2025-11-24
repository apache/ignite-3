//
// Created by Ed on 25.11.2025.
//

#pragma once
#include <cstddef>
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>
#include <unistd.h>

namespace ignite {

/**
 * Owning wrapper around server-side client socket.
 */
class tcp_client_channel {
    int m_cl_fd;
    std::byte m_buf[1024];
    size_t m_pos = 0;
    size_t m_remaining = 0;

public:
    explicit tcp_client_channel(int m_cl_fd)
        : m_cl_fd(m_cl_fd) {}

    ~tcp_client_channel() {
        ::close(m_cl_fd);
    };

    std::vector<std::byte> read_next_n_bytes(size_t n);
private:
    void receive_next_packet();
};
} // namespace ignite