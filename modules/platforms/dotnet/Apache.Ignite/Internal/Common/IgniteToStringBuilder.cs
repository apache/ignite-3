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
using System.Text;

/// <summary>
/// ToString helper.
/// </summary>
internal record IgniteToStringBuilder
{
    private readonly StringBuilder _builder;

    private bool _first = true;

    private bool _closed;

    private IgniteToStringBuilder? _parent;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteToStringBuilder"/> class.
    /// </summary>
    /// <param name="typeName">Type name.</param>
    public IgniteToStringBuilder(string typeName)
        : this(new(), typeName, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteToStringBuilder"/> class.
    /// </summary>
    /// <param name="type">Type.</param>
    public IgniteToStringBuilder(Type type)
        : this(new(), type.Name, null)
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
    /// Appends a property.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="value">Property value.</param>
    /// <returns>This instance.</returns>
    public IgniteToStringBuilder Append(string name, object? value)
    {
        AppendComma();

        _builder.Append(name);
        _builder.Append(" = ");
        _builder.Append(value);

        return this;
    }

    /// <summary>
    /// Appends a property.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="value">Property value.</param>
    /// <typeparam name="T">Value type.</typeparam>
    /// <returns>This instance.</returns>
    public IgniteToStringBuilder AppendList<T>(string name, IEnumerable<T> value)
    {
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
            Append(pair.Key, pair.Value);
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
        if (_closed)
        {
            throw new InvalidOperationException("Builder is already closed.");
        }

        _builder.AppendWithSpace("}");
        _closed = true;
    }
}
