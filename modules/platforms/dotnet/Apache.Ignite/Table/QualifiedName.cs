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

namespace Apache.Ignite.Table;

using System;
using System.IO;

/// <summary>
/// Represents a qualified name of a database object.
/// <para />
/// Schema name and object name should conform to SQL syntax rules for identifiers.
/// <list type="bullet">
/// <item>
/// Identifier must start from any character in the Unicode General Category classes “Lu”, “Ll”, “Lt”, “Lm”, “Lo”, or “Nl”.</item>
/// <item>
/// Identifier character (expect the first one) may be U+00B7 (middle dot), or any character in the Unicode General Category
/// classes “Mn”, “Mc”, “Nd”, “Pc”, or “Cf”.
/// </item>
/// <item>
/// Identifier that contains any other characters must be quoted with double-quotes.
/// </item>
/// <item>
/// Double-quote inside the identifier must be encoded as 2 consequent double-quote chars.
/// </item>
/// </list>
/// </summary>
public record struct QualifiedName
{
    /// <summary>
    /// Default schema name.
    /// </summary>
    public const string DefaultSchemaName = "PUBLIC";

    /// <summary>
    /// Separator character between schema and object names.
    /// </summary>
    public const char SeparatorChar = '.';

    /// <summary>
    /// Quote character for identifiers.
    /// </summary>
    public const char QuoteChar = '"';

    /// <summary>
    /// Initializes a new instance of the <see cref="QualifiedName"/> struct.
    /// </summary>
    /// <param name="schemaName">Schema name. When null, default schema name is assumed (see <see cref="DefaultSchemaName"/>.</param>
    /// <param name="objectName">Object name. Can not be null or empty.</param>
    public QualifiedName(string? schemaName, string objectName)
    {
        schemaName ??= DefaultSchemaName;

        VerifyObjectIdentifier(schemaName);
        VerifyObjectIdentifier(objectName);

        SchemaName = schemaName;
        ObjectName = objectName;
    }

    /// <summary>
    /// Gets the schema name.
    /// </summary>
    public string SchemaName { get; }

    /// <summary>
    /// Gets the object name.
    /// </summary>
    public string ObjectName { get; }

    /// <summary>
    /// Parses a qualified name from a string.
    /// </summary>
    /// <param name="simpleOrCanonicalName">Simple or canonical name.</param>
    /// <returns>Parsed qualified name.</returns>
    public static QualifiedName Parse(string simpleOrCanonicalName)
    {
        VerifyObjectIdentifier(simpleOrCanonicalName);

        // TODO: Parsing with quoted name support.
        var parts = simpleOrCanonicalName.Split('.');
        return new QualifiedName(parts[0], parts[1]);
    }

    private static void VerifyObjectIdentifier(string identifier) =>
        ArgumentException.ThrowIfNullOrEmpty(identifier);

    private static int IndexOfSeparatorChar(string name)
    {
        bool quoted = name[0] == QuoteChar;
        int pos = quoted ? 1 : 0;

        while (true)
        {
            char ch = name[pos];

            if (ch == QuoteChar)
            {
                if (!quoted)
                {
                    throw new FormatException($"Identifier is not quoted, but contains quote character at position {pos}: {name}");
                }

                char? nextCh = pos + 1 < name.Length ? name[pos + 1] : null;
                if (nextCh == QuoteChar)
                {
                    // Escaped quote.
                    pos+= 2;
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
            }
        }

        // TODO: Find dot with quoted name support.
        return -1;
    }

    private static bool IsIdentifierStart(char c) =>
        char.IsLetter(c) || c == '_';
}
