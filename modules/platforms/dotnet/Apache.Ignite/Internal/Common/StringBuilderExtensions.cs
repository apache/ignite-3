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

namespace Apache.Ignite.Internal.Common;

using System.Text;

/// <summary>
/// Extension methods for <see cref="StringBuilder"/> class.
/// </summary>
internal static class StringBuilderExtensions
{
    /// <summary>
    /// Removes all the trailing white-space characters from the current string builder.
    /// </summary>
    /// <param name="sb">String builder.</param>
    /// <returns>Same instance for chaining.</returns>
    public static StringBuilder TrimEnd(this StringBuilder sb)
    {
        while (sb.Length > 0 && char.IsWhiteSpace(sb[^1]))
        {
            sb.Length--;
        }

        return sb;
    }

    /// <summary>
    /// Appends a string to the string builder, ensuring that there is a whitespace character between existing and appended content.
    /// </summary>
    /// <param name="sb">String builder.</param>
    /// <param name="str">String to append.</param>
    /// <returns>Same instance for chaining.</returns>
    public static StringBuilder AppendWithSpace(this StringBuilder sb, string str)
    {
        if (!char.IsWhiteSpace(sb[^1]))
        {
            sb.Append(' ');
        }

        sb.Append(str);

        return sb;
    }
}
