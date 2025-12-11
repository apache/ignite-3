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

using System.Diagnostics.CodeAnalysis;

[SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Tests.")]
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Tests.")]
[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
[SuppressMessage("Design", "CA1008:Enums should have zero value", Justification = "Tests.")]
[SuppressMessage("Design", "CA1028:Enum Storage should be Int32", Justification = "Tests.")]
public static class PocoEnums
{
    public record PocoIntEnum(long Key, IntEnum Int32);

    public record PocoIntEnumNullable(long Key, IntEnum? Int32);

    public record PocoShortEnum(long Key, ShortEnum Int16);

    public record PocoShortEnumNullable(long Key, ShortEnum? Int16);

    public record PocoLongEnum(long Key, LongEnum Int64);

    public record PocoLongEnumNullable(long Key, LongEnum? Int64);

    public record PocoByteEnum(long Key, ByteEnum Int8);

    public record PocoByteEnumNullable(long Key, ByteEnum? Int8);

    public record PocoUnsignedByteEnum(long Key, UnsignedByteEnum Int8);

    public enum IntEnum
    {
        Foo = 1,
        Bar = 3
    }

    public enum ShortEnum : short
    {
        Foo = 1,
        Bar = 3
    }

    public enum LongEnum : long
    {
        Foo = 1,
        Bar = 3
    }

    public enum ByteEnum : sbyte
    {
        Foo = 1,
        Bar = 3
    }

    public enum UnsignedByteEnum : byte
    {
        Foo = 1,
        Bar = 3
    }
}
