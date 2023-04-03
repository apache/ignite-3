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
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="object.ToString"/> methods.
/// </summary>
public class ToStringTests
{
    private static readonly List<Type> PublicFacingTypes = GetPublicFacingTypes().ToList();

    [Test]
    public void TestAllPublicFacingTypesHaveConsistentToString()
    {
        // TODO:
        // 1. Get all public types.
        // For interfaces, get internal types implementing them.
        // For abstract classes, get internal types deriving from them.
        // 2. Check that all types have ToString() method, which uses 'TypeName { Property = Value }' format.
        // We can introduce IgniteToStringBuilder for that.
        // 3. Check that GetPublicFacingTypes returns something.
        foreach (var type in GetPublicFacingTypes())
        {
            var path = GetSourcePath(type);

            Console.WriteLine(path);
            Assert.IsTrue(File.Exists(path), path);
        }
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
        var typeName = type.Name;

        if (type.IsGenericType || type.IsGenericTypeDefinition)
        {
            typeName = typeName[..typeName.IndexOf('`')];
        }

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

            if (type.IsInterface || type.IsAbstract || type.IsEnum)
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
