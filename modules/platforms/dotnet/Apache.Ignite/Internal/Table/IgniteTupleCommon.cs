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

namespace Apache.Ignite.Internal.Table;

using System;

/// <summary>
/// Common tuple utilities.
/// </summary>
internal static class IgniteTupleCommon
{
    /// <summary>
    /// Parses column name. Removes quotes and converts to upper case.
    /// </summary>
    /// <param name="name">Name.</param>
    /// <returns>Parsed name.</returns>
    public static string ParseColumnName(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Column name can not be null or empty.");
        }

        if (name.Length > 2 && name.StartsWith('"') && name.EndsWith('"'))
        {
            return name.Substring(1, name.Length - 2);
        }

        return name.ToUpperInvariant();
    }
}
