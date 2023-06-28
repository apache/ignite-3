namespace Apache.Ignite.Tests;

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

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

}
