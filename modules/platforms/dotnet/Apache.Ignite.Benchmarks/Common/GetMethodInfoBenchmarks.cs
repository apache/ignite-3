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
/// </summary>
public class GetMethodInfoBenchmarks
{
    [Benchmark]
    public MethodInfo ByName() =>
        (MethodInfo)typeof(Queryable).GetMember(nameof(Queryable.Any)).Single(x => ((MethodInfo)x).GetParameters().Length == 2);

    [Benchmark]
    public MethodInfo ByExpression() =>
        new Func<IQueryable<object>, Expression<Func<object, bool>>, bool>(Queryable.Any).GetMethodInfo().GetGenericMethodDefinition();
}
