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

using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests request balancing across active connections.
/// When client is connected to multiple nodes, requests will use all available connections in a round-robin fashion
/// (unless an operation is tied to a specific connection, like a transaction or a cursor).
/// </summary>
public class RequestBalancingTests
{
    [Test]
    public async Task TestRequestsAreRoundRobinBalanced()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }
}
