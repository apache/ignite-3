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
    /// Initializes a new instance of the <see cref="QualifiedName"/> struct.
    /// </summary>
    /// <param name="schemaName">Schema name.</param>
    /// <param name="objectName">Object name.</param>
    public QualifiedName(string schemaName, string objectName)
    {
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
}
