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

namespace Apache.Ignite.Internal.Proto.MsgPack;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// MsgPack type codes.
/// </summary>
[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:Elements should be documented", Justification = "Self-explanatory.")]
[SuppressMessage("Naming", "CA1720:Identifier contains type name", Justification = "MsgPack names.")]
internal static class MsgPackCode
{
    public const byte MinFixInt = 0x00; // 0
    public const byte MaxFixInt = 0x7f; // 127
    public const byte MinFixMap = 0x80; // 128
    public const byte MaxFixMap = 0x8f; // 143
    public const byte MinFixArray = 0x90; // 144
    public const byte MaxFixArray = 0x9f; // 159
    public const byte MinFixStr = 0xa0; // 160
    public const byte MaxFixStr = 0xbf; // 191
    public const byte Nil = 0xc0;
    public const byte NeverUsed = 0xc1;
    public const byte False = 0xc2;
    public const byte True = 0xc3;
    public const byte Bin8 = 0xc4;
    public const byte Bin16 = 0xc5;
    public const byte Bin32 = 0xc6;
    public const byte Ext8 = 0xc7;
    public const byte Ext16 = 0xc8;
    public const byte Ext32 = 0xc9;
    public const byte Float32 = 0xca;
    public const byte Float64 = 0xcb;
    public const byte UInt8 = 0xcc;
    public const byte UInt16 = 0xcd;
    public const byte UInt32 = 0xce;
    public const byte UInt64 = 0xcf;
    public const byte Int8 = 0xd0;
    public const byte Int16 = 0xd1;
    public const byte Int32 = 0xd2;
    public const byte Int64 = 0xd3;
    public const byte FixExt1 = 0xd4;
    public const byte FixExt2 = 0xd5;
    public const byte FixExt4 = 0xd6;
    public const byte FixExt8 = 0xd7;
    public const byte FixExt16 = 0xd8;
    public const byte Str8 = 0xd9;
    public const byte Str16 = 0xda;
    public const byte Str32 = 0xdb;
    public const byte Array16 = 0xdc;
    public const byte Array32 = 0xdd;
    public const byte Map16 = 0xde;
    public const byte Map32 = 0xdf;
    public const byte MinNegativeFixInt = 0xe0; // 224
    public const byte MaxNegativeFixInt = 0xff; // 255
}
