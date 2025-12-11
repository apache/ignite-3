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

namespace Apache.Ignite.Tests.Common.Table;

using System;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Test user object.
/// </summary>
[SuppressMessage("Microsoft.Naming", "CA1720:AvoidTypeNamesInParameters", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays", Justification = "POCO mapping.")]
public record PocoAllColumnsBigDecimal(
    long Key,
    string? Str,
    sbyte Int8,
    short Int16,
    int Int32,
    long Int64,
    float Float,
    double Double,
    Guid Uuid,
    BigDecimal Decimal);
