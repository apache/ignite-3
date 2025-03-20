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
using Compute;
using Ignite.Compute;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteException.CodeAsString"/>.
/// </summary>
public class ExceptionCodeAsStringTests : IgniteTestsBase
{
    [Test]
    public async Task TestCodeAsStringIsConsistentWithJava()
    {
        var errorCode = ErrorGroups.Common.Internal;
        var dotNetCodeStr = GetCodeAsString(errorCode);
        var javaCodeStr = await GetCodeAsStringJava(errorCode);

        Assert.AreEqual(javaCodeStr, dotNetCodeStr);
    }

    private static string GetCodeAsString(int errorCode)
    {
        var ex = new IgniteException(Guid.Empty, errorCode, null);

        return ex.CodeAsString;
    }

    private async Task<string> GetCodeAsStringJava(int errorCode)
    {
        var nodes = await Client.GetClusterNodesAsync();
        var jobExec = await Client.Compute.SubmitAsync(JobTarget.AnyNode(nodes), ComputeTests.ExceptionCodeAsStringJob, errorCode);
        return await jobExec.GetResultAsync();
    }
}
