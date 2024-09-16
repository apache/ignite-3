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

namespace Apache.Ignite.Internal.Sql;

using Ignite.Sql;

/// <summary>
/// Column metadata.
/// </summary>
/// <param name="Name">Name.</param>
/// <param name="Type">Type.</param>
/// <param name="Precision">Precision, or -1 when not applicable to the current column type.</param>
/// <param name="Scale">Scale.</param>
/// <param name="Nullable">Whether column is nullable.</param>
/// <param name="Origin">Origin.</param>
internal sealed record ColumnMetadata(string Name, ColumnType Type, int Precision, int Scale, bool Nullable, IColumnOrigin? Origin)
    : IColumnMetadata;
