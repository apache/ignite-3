namespace Apache.Ignite.Tests;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using Internal.Buffers;

[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public abstract class IgniteServerBase
{
    private static int ReceiveMessageSize(Socket handler)
    {
        using var buf = ReceiveBytes(handler, 4);
        return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buf.AsMemory().Span));
    }

    private static PooledBuffer ReceiveBytes(Socket socket, int size)
    {
        int received = 0;
        var buf = ByteArrayPool.Rent(size);

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


    [SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "Tests.")]
    [SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "Tests.")]
    public class ConnectionLostException : Exception
    {
    }
}
