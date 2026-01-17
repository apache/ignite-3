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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using Internal;
    using Internal.Buffers;
    using Internal.Proto;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientSocket"/>.
    /// </summary>
    public class ClientSocketTests : IgniteTestsBase
    {
        private static readonly IClientSocketEventListener Listener = new NoOpListener();

        [Test]
        public async Task TestConnectAndSendRequestReturnsResponse()
        {
            using var socket = await ClientSocket.ConnectAsync(GetEndPoint(), GetConfigInternal(), Listener);

            using var requestWriter = ProtoCommon.GetMessageWriter();
            requestWriter.MessageWriter.Write("\"non-existent-table\"");

            using var response = await socket.DoOutInOpAsync(ClientOp.TableGet, requestWriter);
            Assert.IsTrue(response.GetReader().TryReadNil());
        }

        [Test]
        public async Task TestConnectAndSendRequestWithInvalidOpCodeThrowsError()
        {
            using var socket = await ClientSocket.ConnectAsync(GetEndPoint(), GetConfigInternal(), Listener);

            using var requestWriter = ProtoCommon.GetMessageWriter();
            requestWriter.MessageWriter.Write(123);

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await socket.DoOutInOpAsync((ClientOp)1234567, requestWriter));

            StringAssert.Contains("Unexpected operation code: 1234567", ex!.Message);
        }

        [Test]
        public async Task TestDisposedSocketThrowsExceptionOnSend()
        {
            var socket = await ClientSocket.ConnectAsync(GetEndPoint(), GetConfigInternal(), Listener);

            socket.Dispose();

            using var requestWriter = new PooledArrayBuffer();
            requestWriter.MessageWriter.Write(123);

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(
                async () => await socket.DoOutInOpAsync(ClientOp.SchemasGet, requestWriter));

            Assert.IsInstanceOf<ObjectDisposedException>(ex!.InnerException);

            // Multiple dispose is allowed.
            socket.Dispose();
        }

        [Test]
        public void TestConnectWithoutServerThrowsException()
        {
            Assert.CatchAsync(async () => await ClientSocket.ConnectAsync(GetEndPoint(569), GetConfigInternal(), Listener));
        }

        private static SocketEndpoint GetEndPoint(int? serverPort = null) =>
            new(new(IPAddress.Loopback, serverPort ?? ServerPort), string.Empty, string.Empty);

        private static IgniteClientConfigurationInternal GetConfigInternal() =>
            new(new(), Task.FromResult<IgniteApiAccessor>(null!), DnsResolver.Instance, new());

        private class NoOpListener : IClientSocketEventListener
        {
            public void OnAssignmentChanged(long timestamp)
            {
                // No-op.
            }

            public void OnObservableTimestampChanged(long timestamp)
            {
                // No-op.
            }

            public void OnDisconnect(Exception? ex)
            {
                // No-op.
            }
        }
    }
}
