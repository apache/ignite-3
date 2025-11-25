//
// Created by Ed on 25.11.2025.
//

#pragma once
#include <atomic>
#include <cstddef>
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ignite {

/**
 * Owning wrapper around server-side client socket.
 */
class tcp_client_channel {
    int m_srv_fd;
    int m_cl_fd = -1;
    std::byte m_buf[1024];
    size_t m_pos = 0;
    size_t m_remaining = 0;
    std::atomic_bool m_stopped{false};

public:
    explicit tcp_client_channel(int srv_socket_fd)
        : m_srv_fd(srv_socket_fd) {}

    void start();
    void stop();
    std::vector<std::byte> read_next_n_bytes(size_t n);
    void send_message(std::vector<std::byte> msg);

private:
    void receive_next_packet();


};
} // namespace ignite