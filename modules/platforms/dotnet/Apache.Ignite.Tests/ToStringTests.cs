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

using NUnit.Framework;

/// <summary>
/// Tests for <see cref="object.ToString"/> methods.
/// </summary>
public class ToStringTests
{
    [Test]
    public void TestAllPublicFacingTypesHaveConsistentToString()
    {
        // TODO:
        // 1. Get all public types.
        // For interfaces, get internal types implementing them.
        // For abstract classes, get internal types deriving from them.
        // 2. Check that all types have ToString() method, which uses 'TypeName { Property = Value }' format.
        // We can introduce IgniteToStringBuilder for that.
    }
}
