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

namespace Apache.Ignite.Tests.Common.Compute;

using Apache.Ignite.Compute;

public static class JavaJobs
{
    public const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

    public const string Jobs = "org.apache.ignite.internal.runner.app.Jobs";

    public static readonly JobDescriptor<object?, string> NodeNameJob = new(Jobs + "$NodeNameJob");

    public static readonly JobDescriptor<string?, string> ConcatJob = new(Jobs + "$ConcatJob");

    public static readonly JobDescriptor<string, string> ErrorJob = new(Jobs + "$IgniteExceptionJob");

    public static readonly JobDescriptor<object?, object> EchoJob = new(Jobs + "$EchoJob");

    public static readonly JobDescriptor<object, string> ToStringJob = new(Jobs + "$ToStringJob");

    public static readonly JobDescriptor<int, string> SleepJob = new(Jobs + "$SleepJob");

    public static readonly JobDescriptor<string, BigDecimal> DecimalJob = new(Jobs + "$DecimalJob");

    public static readonly JobDescriptor<string, string> CreateTableJob = new(PlatformTestNodeRunner + "$CreateTableJob");

    public static readonly JobDescriptor<string, string> DropTableJob = new(PlatformTestNodeRunner + "$DropTableJob");

    public static readonly JobDescriptor<object, object> ExceptionJob = new(PlatformTestNodeRunner + "$ExceptionJob");

    public static readonly JobDescriptor<string, object> CheckedExceptionJob = new(PlatformTestNodeRunner + "$CheckedExceptionJob");

    public static readonly JobDescriptor<long, int> PartitionJob = new(PlatformTestNodeRunner + "$PartitionJob");

    public static readonly TaskDescriptor<string, string> NodeNameTask = new(Jobs + "$MapReduceNodeNameTask");

    public static readonly TaskDescriptor<int, object?> SleepTask = new(PlatformTestNodeRunner + "$SleepTask");

    public static readonly TaskDescriptor<object?, object?> SplitExceptionTask =
        new(Jobs + "$MapReduceExceptionOnSplitTask");

    public static readonly TaskDescriptor<object?, object?> ReduceExceptionTask =
        new(Jobs + "$MapReduceExceptionOnReduceTask");

    public static readonly JobDescriptor<int, string> ExceptionCodeAsStringJob = new(PlatformTestNodeRunner + "$ExceptionCodeAsStringJob");
}
