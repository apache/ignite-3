// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Sql;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

/// <summary>
/// Ignite database parameter collection.
/// </summary>
public class IgniteDbParameterCollection : DbParameterCollection, IReadOnlyList<IgniteDbParameter>, IList<IgniteDbParameter>
{
    private readonly List<IgniteDbParameter> _parameters = new();

    protected internal IgniteDbParameterCollection()
    {
    }

    bool ICollection<IgniteDbParameter>.Remove(IgniteDbParameter item)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int Count
        => _parameters.Count;

    /// <inheritdoc/>
    public override object SyncRoot
        => ((ICollection)_parameters).SyncRoot;

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
    protected override DbParameter GetParameter(int index)
        => this[index];

    /// <inheritdoc/>
    protected override DbParameter GetParameter(string parameterName)
        => GetParameter(IndexOfChecked(parameterName));

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

    public int IndexOf(IgniteDbParameter item)
    {
        throw new NotImplementedException();
    }

    public virtual void Insert(int index, IgniteDbParameter value)
        => _parameters.Insert(index, value);

    /// <inheritdoc/>
    public override void Remove(object value)
        => Remove((IgniteDbParameter)value);

    public virtual void Remove(IgniteDbParameter value)
        => _parameters.Remove(value);

    /// <inheritdoc/>
    public override void RemoveAt(int index)
        => _parameters.RemoveAt(index);

    /// <inheritdoc/>
    public override void RemoveAt(string parameterName)
        => RemoveAt(IndexOfChecked(parameterName));

    /// <inheritdoc/>
    protected override void SetParameter(int index, DbParameter value)
        => this[index] = (IgniteDbParameter)value;

    /// <inheritdoc/>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOfChecked(parameterName), value);

    private int IndexOfChecked(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
        {
            throw new IndexOutOfRangeException($"Parameter not found: {parameterName}");
        }

        return index;
    }

    public IgniteDbParameter this[int index]
    {
        get => throw new NotImplementedException();
        set => throw new NotImplementedException();
    }
}
