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
using System.IO;
using System.Linq;
using Ignite.Sql;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="object.ToString"/> methods.
/// </summary>
public class ToStringTests
{
    private static readonly List<Type> PublicFacingTypes = GetPublicFacingTypes().ToList();

    private static readonly HashSet<Type> ExcludedTypes =
    [
        typeof(RetryReadPolicy), // Inherits from RetryReadPolicy
        typeof(IgniteDbConnectionStringBuilder)
    ];

    [Test]
    public void TestAllPublicFacingTypesHaveConsistentToString()
    {
        Assert.Multiple(() =>
        {
            foreach (var type in GetPublicFacingTypes())
            {
                if (ExcludedTypes.Contains(type))
                {
                    continue;
                }

                var path = GetSourcePath(type);
                var code = File.ReadAllText(path);

                if (code.Contains("new IgniteToStringBuilder(", StringComparison.Ordinal) ||
                    code.Contains("IgniteToStringBuilder.Build(", StringComparison.Ordinal) ||
                    code.Contains("IIgniteTuple.ToString(this)", StringComparison.Ordinal))
                {
                    continue;
                }

                if (code.Contains("record struct " + GetCleanTypeName(type)))
                {
                    // records provide property-based ToString() in the same format we use.
                    continue;
                }

                Assert.Fail("Missing ToString() override: " + type);
            }
        });
    }

    [Test]
    public void TestPublicFacingTypes()
    {
        CollectionAssert.Contains(PublicFacingTypes, typeof(IgniteTuple));
        CollectionAssert.Contains(PublicFacingTypes, typeof(SslStreamFactory));
        CollectionAssert.Contains(PublicFacingTypes, typeof(Internal.Table.Table));
        CollectionAssert.Contains(PublicFacingTypes, typeof(Internal.Compute.Compute));
    }

    private static string GetSourcePath(Type type)
    {
        var typeName = GetCleanTypeName(type);

        var subNamespace = type.Namespace!
            .Substring("Apache.Ignite".Length)
            .TrimStart('.')
            .Replace('.', Path.DirectorySeparatorChar);

        return Path.Combine(
            TestUtils.SolutionDir,
            "Apache.Ignite",
            subNamespace,
            typeName + ".cs");
    }

    private static string GetCleanTypeName(Type type)
    {
        var typeName = type.Name;

        if (type.IsGenericType || type.IsGenericTypeDefinition)
        {
            typeName = typeName[..typeName.IndexOf('`')];
        }

        return typeName;
    }

    private static IEnumerable<Type> GetPublicFacingTypes()
    {
        var asm = typeof(IIgnite).Assembly;
        var types = asm.GetTypes();

        foreach (var type in types)
        {
            if (typeof(Exception).IsAssignableFrom(type))
            {
                // Exceptions use built-in string conversion.
                continue;
            }

            if (type.IsInterface || type.IsAbstract || type.IsEnum || type.IsRecordClass())
            {
                continue;
            }

            if (type.IsPublic || type.GetInterfaces().Any(x => x.IsPublic && x.Assembly == asm))
            {
                yield return type;
            }
        }
    }
}
