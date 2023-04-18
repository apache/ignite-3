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
/// Tests for <see cref="BasicAuthenticator"/>.
/// </summary>
public class BasicAuthenticatorTests : IgniteTestsBase
{
    private const string EnableAuthenticationJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$EnableAuthenticationJob";

    [TearDown]
    public async Task DisableAuthenticationAfterTest()
    {
        await Client.Compute.ExecuteAsync<object>(await Client.GetClusterNodesAsync(), EnableAuthenticationJob, 0);
    }

    [Test]
    public async Task Test()
    {
        await Client.Compute.ExecuteAsync<object>(await Client.GetClusterNodesAsync(), EnableAuthenticationJob, 1);

        await Client.Tables.GetTablesAsync();
    }
}
