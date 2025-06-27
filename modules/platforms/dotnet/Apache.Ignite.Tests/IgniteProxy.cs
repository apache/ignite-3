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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Internal.Proto;

/// <summary>
/// Proxy for Ignite server with request logging and interception.
/// Provides a way to test request routing while using real Ignite cluster.
/// </summary>
public sealed class IgniteProxy : IgniteServerBase
{
    private readonly Socket _socket;

    private readonly ConcurrentQueue<ClientOp> _ops = new();

    public IgniteProxy(EndPoint targetEndpoint, string nodeName)
    {
        TargetEndpoint = targetEndpoint;
        NodeName = nodeName;

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _socket.Connect(targetEndpoint);
    }

    public string NodeName { get; private init; }

    public EndPoint TargetEndpoint { get; private init; }

    internal IList<ClientOp> ClientOps => _ops.ToList();

    public void ClearOps() => _ops.Clear();

    protected override void Handle(Socket handler, CancellationToken cancellationToken)
    {
        // Handshake.
        {
            // Forward magic from client to server.
            using var magic = ReceiveBytes(handler, 4);
            _socket.Send(magic.AsMemory().Span);

            // Receive handshake from client.
            var msgSize = ReceiveMessageSize(handler);
            using var msg = ReceiveBytes(handler, msgSize);

            // Forward handshake to server.
            _socket.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(msgSize)));
            _socket.Send(msg.AsMemory().Span);

            // Forward magic from server to client.
            using var serverMagic = ReceiveBytes(_socket, 4);
            handler.Send(serverMagic.AsMemory().Span);

            // Receive handshake from server.
            var serverMsgSize = ReceiveMessageSize(_socket);
            using var serverMsg = ReceiveBytes(_socket, serverMsgSize);

            // Forward to client.
            handler.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(serverMsgSize)));
            handler.Send(serverMsg.AsMemory().Span);
        }

        // Separate relay loops for each direction: don't block heartbeats while some request is being processed.
        // Client -> Server.
        var clientToServerRelay = Task.Run(
            () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Receive from client.
                        var msgSize = ReceiveMessageSize(handler);
                        using var msg = ReceiveBytes(handler, msgSize);
                        _ops.Enqueue((ClientOp)msg.GetReader().ReadInt32());

                        // Forward to server.
                        _socket.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(msgSize)));
                        _socket.Send(msg.AsMemory().Span);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Error in IgniteProxy Client -> Server relay (lastOp = {_ops.Last()}: {e}");
                        throw;
                    }
                }
            },
            cancellationToken);

        // Server -> Client.
        var serverToClientRelay = Task.Run(
            () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Receive from server.
                        var serverMsgSize = ReceiveMessageSize(_socket);
                        using var serverMsg = ReceiveBytes(_socket, serverMsgSize);

                        // Forward to client.
                        handler.Send(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(serverMsgSize)));
                        handler.Send(serverMsg.AsMemory().Span);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Error in IgniteProxy Server -> Client relay (lastOp = {_ops.Last()}: {e}");
                        throw;
                    }
                }
            },
            cancellationToken);

        Task.WhenAll(clientToServerRelay, serverToClientRelay).Wait(cancellationToken);
        handler.Disconnect(true);
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
