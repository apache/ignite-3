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
using Ignite.Compute;
using NUnit.Framework;
using Security.Exception;

/// <summary>
/// Tests for <see cref="BasicAuthenticator"/>.
/// </summary>
public class BasicAuthenticatorTests : IgniteTestsBase
{
    private const string EnableAuthnJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$EnableAuthenticationJob";

    private volatile bool _authnEnabled;

    [TearDown]
    public async Task DisableAuthenticationAfterTest() => await EnableAuthn(false);

    [Test]
    public async Task TestAuthnOnClientNoAuthnOnServer()
    {
        var cfg = new IgniteClientConfiguration(GetConfig())
        {
            Authenticator = new BasicAuthenticator
            {
                Username = "u",
                Password = "p"
            }
        };

        using var client = await IgniteClient.StartAsync(cfg);
        await client.GetClusterNodesAsync();
    }

    [Test]
    public async Task TestAuthnOnServerNoAuthnOnClient()
    {
        await EnableAuthn(true);

        var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await IgniteClient.StartAsync(GetConfig()));
        var invalidCredentialsException = (InvalidCredentialsException)ex!.InnerException!;

        Assert.AreEqual("Authentication failed", invalidCredentialsException.Message);
        Assert.AreEqual(ErrorGroups.Authentication.InvalidCredentials, invalidCredentialsException.Code);
    }

    [Test]
    public async Task TestAuthnOnClientAndServer()
    {
        await EnableAuthn(true);

        using var client = await IgniteClient.StartAsync(GetConfig(true));
        await client.GetClusterNodesAsync();
        await client.Tables.GetTablesAsync();
    }

    private static IgniteClientConfiguration GetConfig(bool enableAuthn) =>
        new(GetConfig())
        {
            RetryPolicy = new RetryNonePolicy(),
            Authenticator = enableAuthn
                ? new BasicAuthenticator
                {
                    Username = "user-1",
                    Password = "password-1"
                }
                : null
        };

    private async Task EnableAuthn(bool enable)
    {
        if (enable == _authnEnabled)
        {
            return;
        }

        using var client = await IgniteClient.StartAsync(GetConfig(_authnEnabled));
        var nodes = JobTarget.AnyNode(await client.GetClusterNodesAsync());

        try
        {
            await client.Compute.SubmitAsync(nodes, new JobDescriptor<object>(EnableAuthnJob), enable ? 1 : 0);
        }
        catch (IgniteClientConnectionException)
        {
            // Ignore.
            // As a result of this call, the client may be disconnected from the server due to authn config change.
        }

        if (enable)
        {
            // Wait for the server to apply the configuration change and drop the client connection.
            client.WaitForConnections(0, 3000);
        }
        else
        {
            await TestUtils.WaitForConditionAsync(async () =>
            {
                try
                {
                    // Ensure that all servers have applied the configuration change.
                    foreach (var endpoint in GetConfig().Endpoints)
                    {
                        var cfg = new IgniteClientConfiguration(endpoint);
                        using var client2 = await IgniteClient.StartAsync(cfg);
                    }

                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            });
        }

        _authnEnabled = enable;
    }
}
