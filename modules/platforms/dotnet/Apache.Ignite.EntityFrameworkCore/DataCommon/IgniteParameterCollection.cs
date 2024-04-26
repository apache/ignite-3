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

namespace Apache.Ignite.EntityFrameworkCore.DataCommon;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Apache.Ignite.Sql;

public class IgniteParameterCollection : DbParameterCollection
{
    private readonly List<IgniteParameter> _parameters = new();

    protected internal IgniteParameterCollection()
    {
    }

    public override int Count
        => _parameters.Count;

    public override object SyncRoot
        => ((ICollection)_parameters).SyncRoot;

    public new virtual IgniteParameter this[int index]
    {
        get => _parameters[index];
        set
        {
            if (_parameters[index] == value)
            {
                return;
            }

            _parameters[index] = value;
        }
    }

    public object[] ToObjectArray()
        => _parameters.Select(x => x.Value).ToArray();

    public override int Add(object value)
    {
        _parameters.Add((IgniteParameter)value);

        return Count - 1;
    }

    public virtual IgniteParameter Add(IgniteParameter value)
    {
        _parameters.Add(value);

        return value;
    }

    public virtual IgniteParameter Add(string? parameterName, ColumnType type)
        => Add(new IgniteParameter()
        {
            ParameterName = parameterName,
            ColumnType = type
        });

    public override void AddRange(Array values)
        => AddRange(values.Cast<IgniteParameter>());

    public virtual void AddRange(IEnumerable<IgniteParameter> values)
        => _parameters.AddRange(values);

    public override void Clear()
        => _parameters.Clear();

    public override bool Contains(object value)
        => Contains((IgniteParameter)value);

    public virtual bool Contains(IgniteParameter value)
        => _parameters.Contains(value);

    public override bool Contains(string value)
        => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index)
        => CopyTo((IgniteParameter[])array, index);

    public virtual void CopyTo(IgniteParameter[] array, int index)
        => _parameters.CopyTo(array, index);

    public override IEnumerator GetEnumerator()
        => _parameters.GetEnumerator();

    protected override DbParameter GetParameter(int index)
        => this[index];

    protected override DbParameter GetParameter(string parameterName)
        => GetParameter(IndexOfChecked(parameterName));

    public override int IndexOf(object value)
        => IndexOf((IgniteParameter)value);

    public virtual int IndexOf(IgniteParameter value)
        => _parameters.IndexOf(value);

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

    public override void Insert(int index, object value)
        => Insert(index, (IgniteParameter)value);

    public virtual void Insert(int index, IgniteParameter value)
        => _parameters.Insert(index, value);

    public override void Remove(object value)
        => Remove((IgniteParameter)value);

    public virtual void Remove(IgniteParameter value)
        => _parameters.Remove(value);

    public override void RemoveAt(int index)
        => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
        => RemoveAt(IndexOfChecked(parameterName));

    protected override void SetParameter(int index, DbParameter value)
        => this[index] = (IgniteParameter)value;

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
}
