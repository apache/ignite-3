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

using System.Linq;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for data streamer with .NET receiver.
/// </summary>
public class DataStreamerPlatformReceiverTests : IgniteTestsBase
{
    [TestCaseSource(typeof(TestCases), nameof(TestCases.SupportedArgs))]
    public async Task TestEchoArgsReceiverAllDataTypes(object arg)
    {
        var receiverDesc = new ReceiverDescriptor<object, object>("TodoMyClass")
        {
            Options = new ReceiverExecutionOptions
            {
                ExecutorType = JobExecutorType.DotNetSidecar
            }
        };

        var res = await PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            receiverDesc,
            receiverArg: arg).SingleAsync();

        if (arg is decimal dec)
        {
            arg = new BigDecimal(dec);
        }

        Assert.AreEqual(arg, res);
    }
}
