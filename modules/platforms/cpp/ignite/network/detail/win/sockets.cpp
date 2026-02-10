/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sockets.h"

#include "ignite/network/detail/utils.h"
#include "ignite/network/socket_client.h"

#include <mutex>
#include <sstream>

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::network::detail {

std::string get_socket_error_message(HRESULT error) {
    std::stringstream res;

    res << "error_code=" << error;

    if (error == 0)
        return res.str();

    LPTSTR errorText = NULL;

    DWORD len = FormatMessageA(
        // use system message tables to retrieve error text
        FORMAT_MESSAGE_FROM_SYSTEM
            // allocate buffer on local heap for error text
            | FORMAT_MESSAGE_ALLOCATE_BUFFER
            // We're not passing insertion m_parameters
            | FORMAT_MESSAGE_IGNORE_INSERTS,
        // unused with FORMAT_MESSAGE_FROM_SYSTEM
        NULL, error, MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
        // output
        reinterpret_cast<LPSTR>(&errorText),
        // minimum size for output buffer
        0,
        // arguments - see note
        NULL);

    if (NULL != errorText && len > 0) {
        std::string msg(reinterpret_cast<const char *>(errorText), static_cast<size_t>(len));

        res << ", msg=" << msg;

        LocalFree(errorText);
    }

    return res.str();
}

std::string get_last_socket_error_message() {
    HRESULT last_error = WSAGetLastError();

    return get_socket_error_message(last_error);
}

void try_set_socket_options(SOCKET socket, int buf_size, BOOL no_delay, BOOL out_of_band, BOOL keep_alive) {
    setsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&buf_size), sizeof(buf_size));
    setsockopt(socket, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&buf_size), sizeof(buf_size));

    setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&no_delay), sizeof(no_delay));

    setsockopt(socket, SOL_SOCKET, SO_OOBINLINE, reinterpret_cast<char *>(&out_of_band), sizeof(out_of_band));

    int res = setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<char *>(&keep_alive), sizeof(keep_alive));

    if (keep_alive) {
        if (SOCKET_ERROR == res) {
            // There is no sense in configuring keep alive params if we failed to set up keep-alive mode.
            return;
        }

        // The time in seconds the connection needs to remain idle before starts sending keepalive probes.
        enum { KEEP_ALIVE_IDLE_TIME = 60 };

        // The time in seconds between individual keepalive probes.
        enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL)
        // This option is available starting with Windows 10, version 1709.
        DWORD idleOpt = KEEP_ALIVE_IDLE_TIME;
        DWORD idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;

        setsockopt(socket, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char *>(&idleOpt), sizeof(idleOpt));

        setsockopt(socket, IPPROTO_TCP, TCP_KEEPINTVL, reinterpret_cast<char *>(&idleRetryOpt), sizeof(idleRetryOpt));

#else // use old hardcore WSAIoctl
      // WinSock structure for KeepAlive timing settings
        struct tcp_keepalive settings = {0};
        settings.onoff = 1;
        settings.keepalivetime = KEEP_ALIVE_IDLE_TIME * 1000;
        settings.keepaliveinterval = KEEP_ALIVE_PROBES_PERIOD * 1000;

        // pointers for WinSock call
        DWORD bytesReturned;
        WSAOVERLAPPED overlapped;
        overlapped.hEvent = NULL;

        // Set KeepAlive settings
        WSAIoctl(socket, SIO_KEEPALIVE_VALS, &settings, sizeof(struct tcp_keepalive), NULL, 0, &bytesReturned,
            &overlapped, NULL);
#endif
    }
}

bool set_non_blocking_mode(SOCKET socket_handle, bool non_blocking) {
    ULONG opt = non_blocking ? TRUE : FALSE;
    return ::ioctlsocket(socket_handle, FIONBIO, &opt) != SOCKET_ERROR;
}

void init_wsa() {
    static std::mutex init_mutex;
    static bool network_inited = false;

    if (!network_inited) {
        std::lock_guard<std::mutex> lock(init_mutex);
        if (!network_inited) {
            WSADATA wsaData;

            network_inited = WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;

            if (!network_inited)
                throw ignite_error(
                    error::code::CONNECTION, "Networking initialisation failed: " + get_last_socket_error_message());
        }
    }
}

int wait_on_socket(SOCKET socket, std::int32_t timeout, bool rd) {
    int ready;
    int last_error{0};

    fd_set fds;

    do {
        timeval tv{};

        tv.tv_sec = timeout;

        FD_ZERO(&fds);
        FD_SET(socket, &fds);

        fd_set *readFds = 0;
        fd_set *writeFds = 0;

        if (rd)
            readFds = &fds;
        else
            writeFds = &fds;

        ready = select(static_cast<int>(socket) + 1, readFds, writeFds, NULL, timeout == 0 ? NULL : &tv);

        if (ready == SOCKET_ERROR)
            last_error = WSAGetLastError();

    } while (ready == SOCKET_ERROR && last_error == WSAEINTR);

    if (ready == SOCKET_ERROR)
        return -last_error;

    if (ready == 0)
        return socket_client::wait_result::TIMEOUT;

    return socket_client::wait_result::SUCCESS;
}

} // namespace ignite::network::detail
