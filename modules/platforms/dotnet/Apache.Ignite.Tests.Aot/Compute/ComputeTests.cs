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

namespace Apache.Ignite.Tests.Aot.Compute;

using Common.Compute;
using Ignite.Compute;
using Ignite.Table;
using JetBrains.Annotations;
using Network;
using static Common.Table.TestTables;

public class ComputeTests(IIgniteClient client)
{
    [UsedImplicitly]
    public async Task TestEcho()
    {
        IJobTarget<IEnumerable<IClusterNode>> target = JobTarget.AnyNode(await client.GetClusterNodesAsync());

        IJobExecution<object> exe = await client.Compute.SubmitAsync(target, JavaJobs.EchoJob, "hello");
        object res = await exe.GetResultAsync();

        Assert.AreEqual("hello", res);
    }

    [UsedImplicitly]
    public async Task TestColocated()
    {
        var keyTuple = new IgniteTuple { [KeyCol] = 42L };

        IJobExecution<string> exec = await client.Compute.SubmitAsync(JobTarget.Colocated(TableName, keyTuple), JavaJobs.NodeNameJob, null);
        var res = await exec.GetResultAsync();

        Assert.AreEqual(JavaJobs.PlatformTestNodeRunner, res);
    }

    [UsedImplicitly]
    public async Task TestColocatedPrimitiveKeyThrows()
    {
        try
        {
            await client.Compute.SubmitAsync(JobTarget.Colocated(TableName, 42L), JavaJobs.NodeNameJob, null);
            throw new Exception("Expected exception was not thrown.");
        }
        catch (InvalidOperationException e)
        {
            Assert.AreEqual("Colocated job target requires an IIgniteTuple key when running in trimmed AOT mode.", e.Message);
        }
    }

    [UsedImplicitly]
    public async Task TestTupleWithSchemaRoundTrip()
    {
        var tuple = TestCases.GetTupleWithAllFieldTypes(x => x is not decimal);
        tuple["nested_tuple"] = TestCases.GetTupleWithAllFieldTypes(x => x is not decimal);

        var nodes = JobTarget.AnyNode(await client.GetClusterNodesAsync());
        IJobExecution<object> resExec = await client.Compute.SubmitAsync(nodes, JavaJobs.EchoJob, tuple);
        var res = await resExec.GetResultAsync();

        Assert.AreEqual(tuple, res);
    }

    [UsedImplicitly]
    public async Task TestDeepNestedTupleWithSchemaRoundTrip()
    {
        var tuple = TestCases.GetNestedTuple(100);

        var nodes = JobTarget.AnyNode(await client.GetClusterNodesAsync());
        IJobExecution<object> resExec = await client.Compute.SubmitAsync(nodes, JavaJobs.EchoJob, tuple);
        var res = await resExec.GetResultAsync();

        Assert.AreEqual(tuple, res);
    }
}
