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
using System.Threading.Tasks;
using NUnit.Framework;
using Security;

/// <summary>
/// Tests for <see cref="BasicAuthenticator"/>.
/// </summary>
public class BasicAuthenticatorTests : IgniteTestsBase
{
    private const string EnableAuthenticationJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$EnableAuthenticationJob";

    [TearDown]
    public async Task DisableAuthenticationAfterTest()
    {
        await EnableAuthn(false);

        Assert.DoesNotThrowAsync(async () => await Client.Tables.GetTablesAsync());
    }

    [Test]
    public async Task TestAuthnOnServerNoAuthnOnClient()
    {
        await EnableAuthn(true);

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await Client.Tables.GetTablesAsync());

        var inner = ((AggregateException)ex!.InnerException!).InnerExceptions;
        Assert.AreEqual(2, inner.Count);

        Assert.IsInstanceOf<IgniteClientConnectionException>(inner[0]); // Connection dropped by server on authn config change.
        Assert.IsInstanceOf<IgniteClientConnectionException>(inner[1]); // Connection dropped by server on retry with authn failure.

        Assert.IsInstanceOf<AuthenticationException>(inner[1].InnerException);
        StringAssert.Contains("Authentication failed", inner[1].InnerException!.Message);
    }

    private async Task EnableAuthn(bool enable) =>
        await Client.Compute.ExecuteAsync<object>(await Client.GetClusterNodesAsync(), EnableAuthenticationJob, enable ? 1 : 0);
}
