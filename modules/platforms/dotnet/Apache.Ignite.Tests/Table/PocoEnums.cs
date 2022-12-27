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

namespace Apache.Ignite.Tests.Table;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Test user object.
/// </summary>
[SuppressMessage("Microsoft.Naming", "CA1720:AvoidTypeNamesInParameters", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays", Justification = "POCO mapping.")]
public record PocoEnums(long Key, TestEnum Int32);

[SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Tests.")]
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Tests.")]
public enum TestEnum
{
    None = 0,
    Foo = 1,
    BarBaz = 3
}
