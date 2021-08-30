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
    using System.Net;
    using System.Threading.Tasks;
    using Internal;
    using Internal.Proto;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientSocket"/>.
    /// </summary>
    public class ClientSocketTests : IgniteTestsBase
    {
        [Test]
        public async Task TestConnectAndSendRequest()
        {
            using var socket = await ClientSocket.ConnectAsync(JavaServer.EndPoint);

            using var requestWriter = socket.GetRequestWriter(ClientOp.TablesGet);

            var response = await socket.DoOutInOpAsync(requestWriter);
            var tableCount = response.GetReader().ReadMapHeader();

            Assert.AreEqual(1, tableCount);
        }

        [Test]
        public void TestConnectWithoutServerThrowsException()
        {
            Assert.CatchAsync(async () => await ClientSocket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 569)));
        }
    }
}
