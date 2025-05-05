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

namespace Apache.Ignite.Tests.Compute;

using Ignite.Compute;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="JobDescriptor{TArg,TResult}"/>.
/// </summary>
public class JobDescriptorTests
{
    [Test]
    public void TestJobDescriptorHasDefaultJavaExecutor()
    {
        Assert.AreEqual(JobExecutorType.JavaEmbedded, JobExecutionOptions.Default.ExecutorType);
    }

    [Test]
    public void TestJobDescriptorSetsDotNetExecutorForDotNetJobType()
    {
        JobDescriptor<int, int> jobDesc = new JobDescriptor<int, int>(typeof(JobDescriptorTests));

        Assert.AreEqual(JobExecutorType.DotNetSidecar, jobDesc.Options?.ExecutorType);
    }

    [Test]
    public void TestJobDescriptorSetsDotNetExecutorForDotNetJobClass()
    {
        JobDescriptor<object?, object?> jobDesc = JobDescriptor.Of(new DotNetJobs.EchoJob());

        Assert.AreEqual(JobExecutorType.DotNetSidecar, jobDesc.Options?.ExecutorType);
    }

    [Test]
    public void TestJobDescriptorPropagatesMarshallers()
    {
        Assert.Fail("TODO");
    }
}
