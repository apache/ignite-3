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

namespace Apache.Ignite.Sql;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Internal.Common;

/// <summary>
/// Ignite database parameter collection.
/// </summary>
public sealed class IgniteDbParameterCollection : DbParameterCollection, IReadOnlyList<IgniteDbParameter>, IList<IgniteDbParameter>
{
    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Not exposed.")]
    private readonly List<IgniteDbParameter> _parameters = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbParameterCollection"/> class.
    /// </summary>
    internal IgniteDbParameterCollection()
    {
        // No-op.
    }

    /// <summary>
    /// Gets the number of parameters in the collection.
    /// </summary>
    public override int Count
        => _parameters.Count;

    /// <inheritdoc/>
    public override object SyncRoot
        => ((ICollection)_parameters).SyncRoot;

    /// <summary>
    /// Gets or sets the element at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the element to get or set.</param>
    /// <returns>The element at the specified index.</returns>
    public new IgniteDbParameter this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = value;
    }

    /// <inheritdoc/>
    public override int Add(object value)
    {
        _parameters.Add((IgniteDbParameter)value);

        return Count - 1;
    }

    /// <inheritdoc/>
    public override void AddRange(Array values)
        => _parameters.AddRange(values.Cast<IgniteDbParameter>());

    /// <inheritdoc />
    public void Add(IgniteDbParameter item) => _parameters.Add(item);

    /// <summary>
    /// Removes all parameters from the collection.
    /// </summary>
    public override void Clear()
        => _parameters.Clear();

    /// <inheritdoc />
    public bool Contains(IgniteDbParameter item) => _parameters.Contains(item);

    /// <inheritdoc />
    public void CopyTo(IgniteDbParameter[] array, int arrayIndex) => _parameters.CopyTo(array, arrayIndex);

    /// <inheritdoc/>
    public override void CopyTo(Array array, int index)
        => _parameters.CopyTo((IgniteDbParameter[])array, index);

    /// <inheritdoc/>
    public override bool Contains(object value)
        => _parameters.Contains((IgniteDbParameter)value);

    /// <inheritdoc/>
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <inheritdoc/>
    IEnumerator<IgniteDbParameter> IEnumerable<IgniteDbParameter>.GetEnumerator()
        => _parameters.GetEnumerator();

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator()
        => _parameters.GetEnumerator();

    /// <inheritdoc/>
    public override int IndexOf(object value)
        => _parameters.IndexOf((IgniteDbParameter)value);

    /// <inheritdoc/>
    public override int IndexOf(string parameterName)
    {
        for (var index = 0; index < _parameters.Count; index++)
        {
            if (_parameters[index].ParameterName == parameterName)
            {
                return index;
            }
        }

        return -1;
    }

    /// <inheritdoc/>
    public override void Insert(int index, object value)
        => Insert(index, (IgniteDbParameter)value);

    /// <inheritdoc/>
    public int IndexOf(IgniteDbParameter item)
        => _parameters.IndexOf(item);

    /// <inheritdoc/>
    public void Insert(int index, IgniteDbParameter value)
        => _parameters.Insert(index, value);

    /// <inheritdoc/>
    public override void Remove(object value)
        => Remove((IgniteDbParameter)value);

    /// <inheritdoc/>
    public bool Remove(IgniteDbParameter value)
        => _parameters.Remove(value);

    /// <summary>Removes the item at the specified index.</summary>
    /// <param name="index">The zero-based index of the item to remove.</param>
    public override void RemoveAt(int index)
        => _parameters.RemoveAt(index);

    /// <inheritdoc/>
    public override void RemoveAt(string parameterName)
        => RemoveAt(IndexOfChecked(parameterName));

    /// <inheritdoc/>
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(Count)
            .Build();

    /// <inheritdoc/>
    protected override void SetParameter(int index, DbParameter value)
        => this[index] = (IgniteDbParameter)value;

    /// <inheritdoc/>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOfChecked(parameterName), value);

    /// <inheritdoc/>
    protected override DbParameter GetParameter(int index)
        => this[index];

    /// <inheritdoc/>
    protected override DbParameter GetParameter(string parameterName)
        => GetParameter(IndexOfChecked(parameterName));

    private int IndexOfChecked(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
        {
            throw new InvalidOperationException($"Parameter not found: {parameterName}");
        }

        return index;
    }
}
