//
// Created by Ed on 21.11.2025.
//

// #include <unistd.h>
#pragma once

#include "ignite/protocol/bitmask_feature.h"
#include "ignite/protocol/protocol_version.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"

#include <arpa/inet.h>
#include <atomic>
#include <cstring>
#include <ignite/common/ignite_error.h>
#include <ignite/protocol/utils.h>
#include <iostream>
#include <queue>
#include <thread>
#include <unistd.h>

using namespace ignite;

using raw_msg = std::vector<std::byte>;

class fake_server {
public:
    explicit fake_server(int srv_port = 10800)
        : m_srv_port(srv_port)
        , m_io_thread([this] {
            while (!m_started.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds{10});
            }

            accept_client_connection();

            handle_client_handshake();

            send_server_handshake();

            while (m_started.load()) {
                handle_requests();
            }
        })
    {}

    ~fake_server() {
        m_started.store(false);

        if (m_accepting_conn) {
            ::close(m_srv_fd);
            m_srv_fd = -1;
        }

        m_io_thread.join();

        if (m_cl_fd > 0) {
            ::close(m_cl_fd);
        }

        if (m_srv_fd > 0 ) {
            ::close(m_srv_fd);
        }
    }

    void start() {
        start_socket();

        bind_address_port();

        start_socket_listen();

        std::cout << "fake server started" << std::endl;

        m_started.store(true);
    }

private:
    int m_srv_fd = -1;
    int m_cl_fd = -1;
    std::atomic_bool m_started{false};
    const int m_srv_port;
    std::thread m_io_thread;
    std::atomic_bool m_accepting_conn{false};
    std::byte m_buf[1024];
    bool m_expect_magic = true;
    std::vector<std::byte> m_ongoing_msg;
    std::queue<raw_msg> m_pending_msgs;

    void start_socket() {
        m_srv_fd = socket(AF_INET, SOCK_STREAM, 6);

        if (m_srv_fd < 0) {
            throw ignite::ignite_error("socket failed");
        }
    }

    void bind_address_port() const {
        sockaddr_in srv_addr{};

        srv_addr.sin_family = AF_INET;
        srv_addr.sin_addr.s_addr = INADDR_ANY;
        srv_addr.sin_port = htons(m_srv_port);

        int bind_res = bind(m_srv_fd, reinterpret_cast<sockaddr *>(&srv_addr), sizeof(srv_addr));

        if (bind_res < 0) {
            throw std::runtime_error("bind failed");
        }
    }

    void start_socket_listen() const {
        int listen_res = listen(m_srv_fd, 1);

        if (listen_res < 0) {
            throw std::runtime_error("listen failed");
        }

        std::cout << "fake server is listening on port=" << m_srv_port << "\n";
    }

    void accept_client_connection() {
        sockaddr_in cl_addr{};

        socklen_t addr_len = sizeof(cl_addr);

        std::cout << "waiting for client to connect srv_fd = " << m_srv_fd << std::endl;

        m_accepting_conn.store(true);
        m_cl_fd = accept(m_srv_fd, reinterpret_cast<sockaddr *>(&cl_addr), &addr_len);
        m_accepting_conn.store(false);

        if (m_cl_fd < 0) {
            std::stringstream ss;
            ss << "connection acceptance failed " << strerror(errno);
            throw std::runtime_error(ss.str());
        }

        std::cout << "Client connected\n";
    }

    size_t receive_packet() {
        int received = ::recv(m_cl_fd, m_buf, sizeof(m_buf), 0);

        if (received == 0) {
            throw ignite_error("connection was closed by client");
        }

        return received;
    }

    void parse_message() {

        using protocol::MAGIC_BYTES;

        size_t received = receive_packet();

        size_t msg_offset = 0;
        if (m_expect_magic) {
            std::array<std::byte, MAGIC_BYTES.size()> magic_recv{};

            std::copy_n(m_buf, MAGIC_BYTES.size(), magic_recv.begin());

            if (magic_recv != MAGIC_BYTES) {
                throw ignite_error("fake server handshake failed: incorrect magic bytes");
            }

            msg_offset += MAGIC_BYTES.size();

            m_expect_magic = false;
        }

        int32_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(m_buf + msg_offset);

        msg_offset += 4;

        m_pending_msgs.push({m_buf + msg_offset, m_buf + msg_offset + msg_size});

        msg_offset += msg_size;

        if (msg_offset < received) {
            int32_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(m_buf + msg_offset);
            msg_offset += 4;


        }
    }

    void add_msg_to_queue() {
        size_t off = 0;

        raw_msg msg{};

        size_t remain_to_full_msg = 0;

        size_t remain_in_cur_packet = 0;

        std::array<std::byte, 3> partial_header;
        size_t header_bytes = 0;

        while (true) {
            if (remain_in_cur_packet == 0) {
                remain_in_cur_packet = receive_packet();

                off = 0;
            }

            if (remain_to_full_msg == 0) {

                if (remain_in_cur_packet < 4) {
                    std::copy_n()
                }

                size_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(m_buf + off);

                off += 4;

                remain_in_cur_packet -= 4;

                msg.reserve(msg_size);

                remain_to_full_msg = msg_size;
            }

            size_t bytes_to_consume = std::min(remain_to_full_msg, remain_in_cur_packet);

            std::copy_n(m_buf + off, bytes_to_consume, std::back_inserter(msg));
            remain_to_full_msg -= bytes_to_consume;
            remain_in_cur_packet -= bytes_to_consume;

            if (remain_to_full_msg == 0) {
                m_pending_msgs.push(msg);

                msg.clear();
            }
        }
    }

    int32_t read_next_int32_t() {

    }

    void send_message(std::vector<std::byte> msg) {
        ::send(m_cl_fd, msg.data(), msg.size(), 0);
    }

    void handle_client_handshake() {

        auto msg = parse_message();

        if (msg.size() < 4) {
            throw ignite_error("fake server handshake failed: first message is too short");
        }

        std::byte *buf = msg.data();
        size_t pos = 0;

        pos += 4;

        int32_t msg_size = detail::bytes::load<detail::endian::BIG, int32_t>(buf + pos);

        std::cout << "Handshake message size = " << msg_size << "\n";

        pos += 4;

        bytes_view bv(buf + pos, msg_size);

        protocol::reader reader(bv);

        int16_t ver_major = reader.read_int16();
        int16_t ver_minor = reader.read_int16();
        int16_t ver_patch = reader.read_int16();
        int16_t client_type = reader.read_int16();

        std::cout << "Client version = " << ver_major << '.' << ver_minor << '.' << ver_patch << '\n';
        std::cout << "Client type = " << client_type << '\n';

        auto features_bytes = reader.read_binary();
        std::vector<std::byte> features{features_bytes.begin(), features_bytes.end()};

        // we ignore the rest for now.

        std::cout << std::flush;
    }

    void send_server_handshake() {
        std::vector<std::byte> msg;
        protocol::buffer_adapter buf(msg);
        protocol::writer writer(buf);

        buf.write_raw(protocol::MAGIC_BYTES);
        buf.reserve_length_header();

        auto ver = protocol::protocol_version::get_current();
        writer.write(ver.get_major());
        writer.write(ver.get_minor());
        writer.write(ver.get_patch());

        writer.write_nil(); // No error.

        writer.write(static_cast<int64_t>(0)); // idle_timeout_ms

        writer.write(uuid::random());
        writer.write("fake_server_node");

        writer.write(static_cast<int32_t>(1));
        writer.write(uuid::random());
        writer.write("fake_cluster");

        writer.write(static_cast<int64_t>(424242)); // Observable timestamp: ignore correct value for now.

        //dbms version
        writer.write(static_cast<int8_t>(42));
        writer.write(static_cast<int8_t>(42));
        writer.write(static_cast<int8_t>(42));
        writer.write(static_cast<int8_t>(42));
        writer.write("dbms_version_pre_release");

        bytes_view features(protocol::all_supported_bitmask_features());
        writer.write_binary(features);

        writer.write_map({}); // extensions

        buf.write_length_header();

        send_message(msg);

        std::cout << "Server handshake message sent" << std::endl;
    }

    void handle_requests() {
        auto msg = parse_message();

        std::cout << "Message recieved" << std::endl;
    }
};
