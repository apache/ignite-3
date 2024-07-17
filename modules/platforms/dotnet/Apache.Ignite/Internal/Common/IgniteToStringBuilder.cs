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

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

/// <summary>
/// ToString helper.
/// </summary>
internal record IgniteToStringBuilder
{
    private readonly StringBuilder _builder;

    private readonly IgniteToStringBuilder? _parent;

    private bool _first = true;

    private bool _closed;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteToStringBuilder"/> class.
    /// </summary>
    /// <param name="type">Type.</param>
    public IgniteToStringBuilder(Type type)
        : this(new(), GetTypeName(type), null)
    {
    }

    private IgniteToStringBuilder(StringBuilder builder, string typeName, IgniteToStringBuilder? parent)
    {
        _parent = parent;
        _builder = builder;
        _builder.Append(typeName);
        _builder.Append(" { ");
    }

    /// <summary>
    /// Builds the string representation.
    /// </summary>
    /// <param name="type">Type.</param>
    /// <returns>String.</returns>
    public static string Build(Type type) => GetTypeName(type) + " { }";

    /// <summary>
    /// Appends a property.
    /// </summary>
    /// <param name="value">Property value.</param>
    /// <param name="name">Property name.</param>
    /// <returns>This instance.</returns>
    public IgniteToStringBuilder Append(object? value, [CallerArgumentExpression("value")] string? name = null)
    {
        IgniteArgumentCheck.NotNull(name);

        CheckClosed();
        AppendComma();

        _builder.Append(name);
        _builder.Append(" = ");
        _builder.Append(value);

        return this;
    }

    /// <summary>
    /// Appends a property.
    /// </summary>
    /// <param name="value">Property value.</param>
    /// <param name="name">Property name.</param>
    /// <typeparam name="T">Value type.</typeparam>
    /// <returns>This instance.</returns>
    public IgniteToStringBuilder AppendList<T>(IEnumerable<T> value, [CallerArgumentExpression("value")] string? name = null)
    {
        IgniteArgumentCheck.NotNull(name);

        CheckClosed();
        AppendComma();

        _builder.Append(name);
        _builder.Append(" = [ ");
        bool first = true;

        foreach (var item in value)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                _builder.Append(", ");
            }

            _builder.Append(item);
        }

        _builder.Append(" ]");

        return this;
    }

    /// <summary>
    /// Appends multiple properties.
    /// </summary>
    /// <param name="pairs">Key value pairs.</param>
    /// <typeparam name="T">Value type.</typeparam>
    /// <returns>This instance.</returns>
    public IgniteToStringBuilder AppendAll<T>(IEnumerable<KeyValuePair<string, T>> pairs)
    {
        foreach (var pair in pairs)
        {
            Append(pair.Value, pair.Key);
        }

        return this;
    }

    /// <summary>
    /// Gets a nested builder.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <returns>Builder.</returns>
    public IgniteToStringBuilder BeginNested(string name)
    {
        AppendComma();

        return new(_builder, name, this);
    }

    /// <summary>
    /// Closes the builder.
    /// </summary>
    /// <returns>Parent builder.</returns>
    public IgniteToStringBuilder EndNested()
    {
        if (_parent == null)
        {
            throw new InvalidOperationException("Nested builder must have a parent.");
        }

        Close();

        return _parent;
    }

    /// <summary>
    /// Builds the resulting string.
    /// </summary>
    /// <returns>String representation.</returns>
    public string Build()
    {
        if (_parent != null)
        {
            throw new InvalidOperationException("Nested builder must be closed before building.");
        }

        Close();

        return _builder.ToString();
    }

    /// <inheritdoc/>
    public override string ToString() => Build();

    private static string GetTypeName(Type type)
    {
        if (!type.IsGenericType)
        {
            return type.Name;
        }

        var sb = new StringBuilder(type.Name).Append('[');
        var args = type.GetGenericArguments();

        for (var i = 0; i < args.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            sb.Append(GetTypeName(args[i]));
        }

        sb.Append(']');

        return sb.ToString();
    }

    private void AppendComma()
    {
        if (_first)
        {
            _first = false;
        }
        else
        {
            _builder.Append(", ");
        }
    }

    private void Close()
    {
        if (!_closed)
        {
            _builder.AppendWithSpace("}");
            _closed = true;
        }
    }

    private void CheckClosed()
    {
        if (_closed)
        {
            throw new InvalidOperationException("Builder is already closed.");
        }
    }
}
