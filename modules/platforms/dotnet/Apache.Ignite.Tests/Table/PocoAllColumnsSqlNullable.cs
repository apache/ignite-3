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

using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using NodaTime;

/// <summary>
/// Test user object.
/// </summary>
[SuppressMessage("Microsoft.Naming", "CA1720:AvoidTypeNamesInParameters", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly", Justification = "POCO mapping.")]
[SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays", Justification = "POCO mapping.")]
public record PocoAllColumnsSqlNullable(
    long Key,
    string? Str = null,
    sbyte? Int8 = null,
    short? Int16 = null,
    int? Int32 = null,
    long? Int64 = null,
    [property:Column("FLOAT")] float? Float = null,
    [property:Column("DOUBLE")] double? Double = null,
    [property:Column("DATE")] LocalDate? Date = null,
    [property:Column("TIME")] LocalTime? Time = null,
    [property:Column("DATETIME")] LocalDateTime? DateTime = null,
    [property:Column("TIMESTAMP")] Instant? Timestamp = null,
    [property:Column("BLOB")] byte[]? Blob = null,
    [property:Column("DECIMAL")] decimal? Decimal = null,
    [property:Column("UUID")] Guid? Uuid = null,
    bool? Boolean = null);
