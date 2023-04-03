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
using System.Text;

/// <summary>
/// ToString helper.
/// </summary>
internal ref struct IgniteToStringBuilder
{
    private readonly StringBuilder _builder = new();

    private bool _first = true;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteToStringBuilder"/> struct.
    /// </summary>
    /// <param name="type">Type.</param>
    public IgniteToStringBuilder(Type type)
    {
        _builder.Append(type.Name);
        _builder.Append(" { ");
    }

    /// <summary>
    /// Appends a property.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="value">Property value.</param>
    public void Append(string name, object value)
    {
        if (_first)
        {
            _first = false;
        }
        else
        {
            _builder.Append(", ");
        }

        _builder.Append(name);
        _builder.Append(" = ");
        _builder.Append(value);
    }

    /// <summary>
    /// Builds the resulting string.
    /// </summary>
    /// <returns>String representation.</returns>
    public string Build()
    {
        _builder.AppendWithSpace("}");

        return _builder.ToString();
    }
}
