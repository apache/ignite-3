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
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Internal.Buffers;
using NUnit.Framework;

[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public abstract class IgniteServerBase : IDisposable
{
    private readonly Socket _listener;

    private readonly CancellationTokenSource _cts = new();

    private readonly object _disposeSyncRoot = new();

    private volatile Socket? _handler;

    private bool _disposed;

    private volatile bool _dropNewConnections;

    protected IgniteServerBase(int port = 0)
    {
        _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listener.NoDelay = true;

        _listener.Bind(new IPEndPoint(IPAddress.Loopback, port));
        _listener.Listen(backlog: 1);

        Console.WriteLine($"Fake server started [port={Port}, test={TestContext.CurrentContext.Test.Name}]");

        Task.Run(ListenLoop);
    }

    public int Port => ((IPEndPoint)Listener.LocalEndPoint!).Port;

    public string Endpoint => "127.0.0.1:" + Port;

    public bool DropNewConnections
    {
        get => _dropNewConnections;
        set => _dropNewConnections = value;
    }

    protected Socket Listener => _listener;

    public void DropExistingConnection() => _handler?.Dispose();

    public void Dispose()
    {
        Dispose(true);
    }

    internal static int ReceiveMessageSize(Socket handler)
    {
        using var buf = ReceiveBytes(handler, 4);
        return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buf.AsMemory().Span));
    }

    internal static PooledBuffer ReceiveBytes(Socket socket, int size)
    {
        int received = 0;
        var buf = ByteArrayPool.Rent(size);

        try
        {
            while (received < size)
            {
                var res = socket.Receive(buf, received, size - received, SocketFlags.None);

                if (res == 0)
                {
                    throw new ConnectionLostException();
                }

                received += res;
            }

            return new PooledBuffer(buf, 0, size);
        }
        catch (Exception)
        {
            ByteArrayPool.Return(buf);
            throw;
        }
    }

    protected virtual void Handle(Socket handler, CancellationToken cancellationToken)
    {
        // No-op.
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                _cts.Cancel();
                _handler?.Dispose();
                _listener.Dispose();
                _cts.Dispose();

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }
    }

    private void ListenLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                ListenLoopInternal();
            }
            catch (Exception e)
            {
                if (e is SocketException or ConnectionLostException)
                {
                    continue;
                }

                Console.WriteLine("Error in FakeServer: " + e);
            }
        }
    }

    private void ListenLoopInternal()
    {
        while (!_cts.IsCancellationRequested)
        {
            using Socket handler = _listener.Accept();
            if (DropNewConnections)
            {
                handler.Disconnect(true);
                _handler = null;

                continue;
            }

            _handler = handler;
            handler.NoDelay = true;

            Handle(handler, _cts.Token);
            handler.Disconnect(true);
            _handler = null;
        }
    }

    [SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "Tests.")]
    [SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "Tests.")]
    public class ConnectionLostException : Exception
    {
    }
}
