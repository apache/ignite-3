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

namespace Apache.Ignite.Sql
{
    /// <summary>
    /// Column metadata.
    /// </summary>
    public interface IColumnMetadata
    {
        /// <summary>
        /// Gets the column name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the column type.
        /// </summary>
        ColumnType Type { get; }

        /// <summary>
        /// Gets the column precision, or -1 when not applicable to the current column type.
        /// <para />
        /// </summary>
        /// <returns>
        /// Number of decimal digits for exact numeric types; number of decimal digits in mantissa for approximate numeric types;
        /// number of decimal digits for fractional seconds of datetime types; length in characters for character types;
        /// length in bytes for binary types; length in bits for bit types; 1 for BOOLEAN; -1 if precision is not valid for the type.
        /// </returns>
        int Precision { get; }

        /// <summary>
        /// Gets the column scale.
        /// <returns>
        /// </returns>
        /// Number of digits of scale.
        /// </summary>
        int Scale { get; }

        /// <summary>
        /// Gets a value indicating whether the column is nullable.
        /// </summary>
        bool Nullable { get; }

        /// <summary>
        /// Gets the column origin.
        /// <para />
        /// For example, for "select foo as bar" query, column name will be "bar", but origin name will be "foo".
        /// </summary>
        IColumnOrigin? Origin { get; }
    }
}
