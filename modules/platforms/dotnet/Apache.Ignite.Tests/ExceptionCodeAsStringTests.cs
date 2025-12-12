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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Common.Compute;
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
        var allErrorCodes = GetAllErrorCodes().ToList();
        Assert.That(allErrorCodes.Count, Is.GreaterThan(110));

        foreach (var errorCode in allErrorCodes)
        {
            var dotNetCodeStr = GetCodeAsString(errorCode);
            var javaCodeStr = await GetCodeAsStringJava(errorCode);

            Assert.AreEqual(javaCodeStr, dotNetCodeStr);
            Console.WriteLine(dotNetCodeStr);
        }
    }

    private static string GetCodeAsString(int errorCode)
    {
        var ex = new IgniteException(Guid.Empty, errorCode, null);

        return ex.CodeAsString;
    }

    private static IEnumerable<int> GetAllErrorCodes()
    {
        foreach (var errorGroup in typeof(ErrorGroups).GetNestedTypes())
        {
            // Get all int constants.
            foreach (var field in errorGroup.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (field.FieldType == typeof(int) && field.IsLiteral)
                {
                    yield return (int)field.GetValue(null)!;
                }
            }
        }
    }

    private async Task<string> GetCodeAsStringJava(int errorCode)
    {
        var nodes = await Client.GetClusterNodesAsync();
        var jobExec = await Client.Compute.SubmitAsync(JobTarget.AnyNode(nodes), JavaJobs.ExceptionCodeAsStringJob, errorCode);
        return await jobExec.GetResultAsync();
    }
}
