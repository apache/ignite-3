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

namespace Apache.Ignite.Tests;

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

/// <summary>
/// Proxy for Ignite server with request logging and interception.
/// </summary>
public sealed class IgniteProxy : IgniteServerBase
{
    private readonly Socket _socket;

    public IgniteProxy(IPEndPoint endPoint)
    {
        EndPoint = endPoint;

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _socket.Connect(endPoint);
    }

    public IPEndPoint EndPoint { get; private init; }

    protected override void Handle(Socket handler, CancellationToken cancellationToken)
    {
        using var magic = ReceiveBytes(handler, 4);
        _socket.Send(magic.AsMemory().Span);

        using var serverMagic = ReceiveBytes(_socket, 4);
        handler.Send(serverMagic.AsMemory().Span);

        while (!cancellationToken.IsCancellationRequested)
        {
            // Receive from client.
            var msgSize = ReceiveMessageSize(handler);
            using var msg = ReceiveBytes(handler, msgSize);

            // Forward to server.
            _socket.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(msgSize)));
            _socket.Send(msg.AsMemory().Span);

            // Receive from server.
            var serverMsgSize = ReceiveMessageSize(_socket);
            using var serverMsg = ReceiveBytes(_socket, serverMsgSize);

            // Forward to client.
            handler.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(serverMsgSize)));
            handler.Send(serverMsg.AsMemory().Span);
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing)
        {
            _socket.Dispose();
        }
    }
}
