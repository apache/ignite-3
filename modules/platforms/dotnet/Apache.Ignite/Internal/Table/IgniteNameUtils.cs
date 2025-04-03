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
/// Ignite name utilities.
/// </summary>
internal static class IgniteNameUtils
{
    /// <summary>
    /// Separator character between schema and object names.
    /// </summary>
    public const char SeparatorChar = '.';

    /// <summary>
    /// Quote character for identifiers.
    /// </summary>
    public const char QuoteChar = '"';

    /// <summary>
    /// Verifies that the specified identifier is not null or empty.
    /// </summary>
    /// <param name="identifier">Identifier.</param>
    public static void VerifyObjectIdentifier(string identifier) =>
        ArgumentException.ThrowIfNullOrEmpty(identifier);

    /// <summary>
    /// Unquotes the specified identifier, or converts it to upper case if it is not quoted.
    /// </summary>
    /// <param name="identifier">Identifier.</param>
    /// <returns>Unquoted or uppercased identifier.</returns>
    public static string Unquote(ReadOnlyMemory<char> identifier)
    {
        if (identifier.IsEmpty)
        {
            return string.Empty;
        }

        return identifier.Span[0] == QuoteChar
            ? identifier[1..^1].ToString().Replace("\"\"", "\"", StringComparison.Ordinal) // Escaped quotes are rare, don't optimize.
            : identifier.ToStringUpperInvariant();
    }

    /// <summary>
    /// Wraps the given name with double quotes if it is not uppercased non-quoted name,
    /// e.g. "myColumn" -> "\"myColumn\"", "MYCOLUMN" -> "MYCOLUMN".
    /// </summary>
    /// <param name="name">Name.</param>
    /// <returns>Quoted name.</returns>
    public static string QuoteIfNeeded(string name)
    {
        // TODO: Implement.
        return name;
    }

    /// <summary>
    /// Converts a memory of chars to an uppercase string.
    /// </summary>
    /// <param name="chars">Chars.</param>
    /// <returns>Uppercased string.</returns>
    public static string ToStringUpperInvariant(this ReadOnlyMemory<char> chars) =>
        string.Create(chars.Length, chars, (span, args) => args.Span.ToUpperInvariant(span));

    /// <summary>
    /// Parses the identifier and returns it unquoted.
    /// </summary>
    /// <param name="identifier">Identifier.</param>
    /// <returns>Parsed identifier.</returns>
    public static string ParseIdentifier(ReadOnlyMemory<char> identifier)
    {
        if (IndexOfSeparatorChar(identifier.Span) is var separatorPos && separatorPos != -1)
        {
            throw new ArgumentException($"Unexpected separator at position {separatorPos}: '{identifier}'");
        }

        return Unquote(identifier);
    }

    /// <summary>
    /// Returns a value indicating whether the specified character is a valid identifier start character.
    /// An identifier start is any character in the Unicode General Category classes “Lu”, “Ll”, “Lt”, “Lm”, “Lo”, or “Nl”.
    /// </summary>
    /// <param name="c">Char.</param>
    /// <returns>Whether the specified character is a valid identifier start character.</returns>
    public static bool IsIdentifierStart(char c) =>
        char.IsLetter(c) || c == '_';

    /// <summary>
    /// Returns a value indicating whether the specified character is a valid identifier extend character.
    /// An identifier extend is U+00B7, or any character in the Unicode General Category classes “Mn”, “Mc”, “Nd”, “Pc”, or “Cf”.
    /// </summary>
    /// <param name="c">Char.</param>
    /// <returns>Whether the specified character is a valid identifier extend character.</returns>
    public static bool IsIdentifierExtend(char c)
    {
        // Direct port from Java.
        return c == ('·' & 0xff) /* “Middle Dot” character */
               || ((((1 << 6)
                     | (1 << 8)
                     | (1 << 9)
                     | (1 << 23)
                     | (1 << 16)) >> (int)char.GetUnicodeCategory(c)) & 1) != 0;
    }

    /// <summary>
    /// Finds the index of the first <see cref="SeparatorChar"/> in the specified identifier, respecting quotes.
    /// </summary>
    /// <param name="name">Identifier.</param>
    /// <returns>Index of the <see cref="SeparatorChar"/>, or -1 when not found.</returns>
    public static int IndexOfSeparatorChar(ReadOnlySpan<char> name)
    {
        if (name.IsEmpty)
        {
            return -1;
        }

        bool quoted = name[0] == QuoteChar;
        int pos = quoted ? 1 : 0;

        for (; pos < name.Length; pos++)
        {
            char ch = name[pos];

            if (ch == QuoteChar)
            {
                if (!quoted)
                {
                    throw new ArgumentException($"Identifier is not quoted, but contains quote character at position {pos}: '{name}'");
                }

                char? nextCh = pos + 1 < name.Length ? name[pos + 1] : null;
                if (nextCh == QuoteChar)
                {
                    // Escaped quote.
                    pos++;
                    continue;
                }

                if (nextCh == SeparatorChar)
                {
                    // End of quoted identifier, separator follows.
                    return pos + 1;
                }

                if (nextCh == null)
                {
                    // End of quoted identifier, no separator.
                    return -1;
                }

                throw new ArgumentException($"Unexpected character '{ch}' after quote at position {pos}: '{name}'");
            }

            if (ch == SeparatorChar)
            {
                if (quoted)
                {
                    continue;
                }

                return pos;
            }

            if (!quoted && !IsIdentifierStart(ch) && !IsIdentifierExtend(ch))
            {
                throw new ArgumentException($"Unexpected character '{ch}' at position {pos}: '{name}'");
            }
        }

        if (quoted)
        {
            throw new ArgumentException($"Missing closing quote: '{name}");
        }

        return -1;
    }
}
