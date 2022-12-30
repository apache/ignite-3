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

namespace Apache.Ignite.Benchmarks.Common;

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BenchmarkDotNet.Attributes;

/// <summary>
/// Compares performance of different ways of getting a <see cref="MethodInfo"/>.
/// |       Method |     Mean |    Error |   StdDev |  Gen 0 | Allocated |
/// |------------- |---------:|---------:|---------:|-------:|----------:|
/// |       ByName | 375.4 ns |  5.77 ns |  5.40 ns | 0.0052 |     152 B |
/// | ByExpression | 928.9 ns | 16.68 ns | 33.70 ns | 0.0210 |     584 B |.
/// </summary>
[MemoryDiagnoser]
public class GetMethodInfoBenchmarks
{
    [Benchmark]
    public MethodInfo ByName()
    {
        foreach (var memberInfo in typeof(Queryable).GetMember(nameof(Queryable.Any)))
        {
            if (memberInfo is MethodInfo mi && mi.GetParameters().Length == 2)
            {
                return mi;
            }
        }

        return null!;
    }

    [Benchmark]
    public MethodInfo ByExpression() =>
        new Func<IQueryable<object>, Expression<Func<object, bool>>, bool>(Queryable.Any).GetMethodInfo().GetGenericMethodDefinition();
}
