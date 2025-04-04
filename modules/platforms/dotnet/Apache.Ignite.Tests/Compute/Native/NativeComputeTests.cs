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

namespace Apache.Ignite.Tests.Compute.Native;

using System.Threading.Tasks;
using Ignite.Compute;
using NUnit.Framework;

/// <summary>
/// Tests for .NET Native Compute API.
/// </summary>
public class NativeComputeTests : IgniteTestsBase
{
    [Test]
    public async Task TestSimpleJob()
    {
        JobDescriptor<object?, string?> jobDesc = new ToStringJob().GetDescriptor() with
        {
            Options = new JobExecutionOptions { MaxRetries = 2 }
        };

        var target = JobTarget.AnyNode(await Client.GetClusterNodesAsync());
        IJobExecution<string?> jobExec = await Client.Compute.SubmitAsync(target, jobDesc, 123);
        string? jobRes = await jobExec.GetResultAsync();

        Assert.AreEqual("123", jobRes);
    }

    private class ToStringJob : IComputeJob<object?, string?>
    {
        public async ValueTask<string?> ExecuteAsync(IJobExecutionContext context, object? arg)
        {
            // await context.Ignite.Tables.GetTablesAsync();
            await Task.Delay(1);
            return arg?.ToString();
        }
    }
}
