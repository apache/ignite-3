namespace Apache.Ignite.Tests;

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Proxy for Ignite server with request logging and interception.
/// </summary>
public sealed class IgniteProxy : IDisposable
{
    private readonly IPEndPoint _endPoint;
    private readonly Socket _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _disposeSyncRoot = new();
    private volatile Socket? _handler;
    private bool _disposed;

    public IgniteProxy(IPEndPoint endPoint)
    {
        _endPoint = endPoint;
        _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listener.NoDelay = true;

        _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _listener.Listen(backlog: 1);
        Task.Run(ListenLoop);
    }

    public void Dispose()
    {
        // TODO: Extract IgniteServerBase class.
        lock (_disposeSyncRoot)
        {
            if (_disposed)
            {
                return;
            }

            _cts.Cancel();
            _handler?.Dispose();
            _listener.Disconnect(false);
            _listener.Dispose();
            _cts.Dispose();

            _disposed = true;
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
                if (e is SocketException)
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
            _handler = handler;
            handler.NoDelay = true;

            // Read handshake.
            using var magic = ReceiveBytes(handler, 4);
            var msgSize = ReceiveMessageSize(handler);
            using var handshake = ReceiveBytes(handler, msgSize);
        }
    }
}
