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

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;

[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public static class TestJobs
{
    public static readonly JobDescriptor<int, int> AddOne = new(typeof(AddOneJob).AssemblyQualifiedName!);
    public static readonly JobDescriptor<int, int> NoCtor = new(typeof(NoCtorJob).AssemblyQualifiedName!);

    public class AddOneJob : IComputeJob<int, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg + 1);
    }

    public class NoCtorJob : IComputeJob<int, int>
    {
        public NoCtorJob(int ctorArg)
        {
            // No-op.
        }

        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }
}
