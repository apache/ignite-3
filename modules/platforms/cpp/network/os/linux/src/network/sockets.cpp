/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <fcntl.h>

#include <cerrno>
#include <cstring>

#include <sstream>

#include "network/sockets.h"

namespace ignite::network::sockets
{

std::string getSocketErrorMessage(int error)
{
    std::stringstream res;

    res << "error_code=" << error;

    if (error == 0)
        return res.str();

    char errBuf[1024] = { 0 };

    const char* errStr = strerror_r(error, errBuf, sizeof(errBuf));
    if (errStr)
        res << ", msg=" << errStr;

    return res.str();
}

std::string getLastSocketErrorMessage()
{
    int lastError = errno;

    return getSocketErrorMessage(lastError);
}

void trySetSocketOptions(int socketFd, int bufSize, bool noDelay, bool outOfBand, bool keepAlive)
{
    setsockopt(socketFd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));
    setsockopt(socketFd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));

    int iNoDelay = noDelay ? 1 : 0;
    setsockopt(socketFd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&iNoDelay), sizeof(iNoDelay));

    int iOutOfBand = outOfBand ? 1 : 0;
    setsockopt(socketFd, SOL_SOCKET, SO_OOBINLINE,
        reinterpret_cast<char*>(&iOutOfBand), sizeof(iOutOfBand));

    int iKeepAlive = keepAlive ? 1 : 0;
    int res = setsockopt(socketFd, SOL_SOCKET, SO_KEEPALIVE,
        reinterpret_cast<char*>(&iKeepAlive), sizeof(iKeepAlive));

    if (SOCKET_ERROR == res)
    {
        // There is no sense in configuring keep alive params if we failed to set up keep alive mode.
        return;
    }

    // The time in seconds the connection needs to remain idle before starts sending keepalive probes.
    enum { KEEP_ALIVE_IDLE_TIME = 60 };

    // The time in seconds between individual keepalive probes.
    enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

    int idleOpt = KEEP_ALIVE_IDLE_TIME;
    int idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;
#ifdef __APPLE__
    setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPALIVE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#else
    setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#endif

    setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPINTVL,
        reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));
}

bool setNonBlockingMode(int socketFd, bool nonBlocking)
{
    int flags = fcntl(socketFd, F_GETFL, 0);
    if (flags == -1)
        return false;

    bool currentNonBlocking = flags & O_NONBLOCK;
    if (nonBlocking == currentNonBlocking)
        return true;

    flags ^= O_NONBLOCK;
    int res = fcntl(socketFd, F_SETFL, flags);

    return res != -1;
}

} // namespace ignite::network::sockets
