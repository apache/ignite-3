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

#include <cerrno>
#include <cstring>
#include <sstream>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace ignite::network::detail {

std::string get_socket_error_message(int error) {
    std::stringstream res;

    res << "error_code=" << error;

    if (error == 0)
        return res.str();

    char err_buf[1024] = {0};

    const char *err_str = strerror_r(error, err_buf, sizeof(err_buf));
    if (err_str)
        res << ", msg=" << err_str;

    return res.str();
}

std::string get_last_socket_error_message() {
    int last_error = errno;

    return get_socket_error_message(last_error);
}

void try_set_socket_options(int socket_fd, int buf_size, bool no_delay, bool out_of_band, bool keep_alive) {
    setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&buf_size), sizeof(buf_size));
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&buf_size), sizeof(buf_size));

    int iNoDelay = no_delay ? 1 : 0;
    setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&iNoDelay), sizeof(iNoDelay));

    int iOutOfBand = out_of_band ? 1 : 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_OOBINLINE, reinterpret_cast<char *>(&iOutOfBand), sizeof(iOutOfBand));

    int iKeepAlive = keep_alive ? 1 : 0;
    int res = setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<char *>(&iKeepAlive), sizeof(iKeepAlive));

    if (SOCKET_ERROR == res) {
        // There is no sense in configuring keep alive params if we failed to set up keep alive mode.
        return;
    }

    // The time in seconds the connection needs to remain idle before starts sending keepalive probes.
    enum { KEEP_ALIVE_IDLE_TIME = 60 };

    // The time in seconds between individual keepalive probes.
    enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

    int idle_opt = KEEP_ALIVE_IDLE_TIME;
    int idle_retry_opt = KEEP_ALIVE_PROBES_PERIOD;
#ifdef __APPLE__
    setsockopt(socket_fd, IPPROTO_TCP, TCP_KEEPALIVE, reinterpret_cast<char *>(&idle_opt), sizeof(idle_opt));
#else
    setsockopt(socket_fd, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char *>(&idle_opt), sizeof(idle_opt));
#endif

    setsockopt(socket_fd, IPPROTO_TCP, TCP_KEEPINTVL, reinterpret_cast<char *>(&idle_retry_opt), sizeof(idle_retry_opt));
}

bool set_non_blocking_mode(int socket_fd, bool non_blocking) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1)
        return false;

    bool current_non_blocking = flags & O_NONBLOCK;
    if (non_blocking == current_non_blocking)
        return true;

    flags ^= O_NONBLOCK;
    int res = fcntl(socket_fd, F_SETFL, flags);

    return res != -1;
}

} // namespace ignite::network::detail
