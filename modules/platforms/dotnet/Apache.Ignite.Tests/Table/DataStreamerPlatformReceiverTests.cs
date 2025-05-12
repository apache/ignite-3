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

namespace Apache.Ignite.Tests.Table;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for data streamer with .NET receiver.
/// </summary>
public class DataStreamerPlatformReceiverTests : IgniteTestsBase
{
    private DeploymentUnit _defaultTestUnit = null!;

    [OneTimeSetUp]
    public async Task DeployDefaultUnit() => _defaultTestUnit = await ManagementApi.DeployTestsAssembly();

    [OneTimeTearDown]
    public async Task UndeployDefaultUnit() => await ManagementApi.UnitUndeploy(_defaultTestUnit);

    [TestCaseSource(typeof(TestCases), nameof(TestCases.SupportedArgs))]
    public async Task TestEchoArgsReceiverAllDataTypes(object arg)
    {
        var res = await PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            DotNetReceivers.EchoArgs with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: arg).SingleAsync();

        if (arg is decimal dec)
        {
            arg = new BigDecimal(dec);
        }

        Assert.AreEqual(arg, res);
    }

    [Test]
    public void TestMissingClass()
    {
        var receiverDesc = new ReceiverDescriptor<object, object>("BadClass")
        {
            Options = new ReceiverExecutionOptions
            {
                ExecutorType = JobExecutorType.DotNetSidecar
            }
        };

        IAsyncEnumerable<object> resStream = PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            receiverDesc,
            receiverArg: "arg");

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await resStream.SingleAsync());
        Assert.AreEqual(".NET job failed: Type 'BadClass' not found in the specified deployment units.", ex.Message);
        Assert.AreEqual(1, ex.FailedItems.Count);
    }
}
