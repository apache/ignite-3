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

namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteQueryStringFactory : IRelationalQueryStringFactory
{
    private readonly IRelationalTypeMappingSource _typeMapper;

    public IgniteQueryStringFactory(IRelationalTypeMappingSource typeMapper)
    {
        _typeMapper = typeMapper;
    }

    public virtual string Create(DbCommand command)
    {
        if (command.Parameters.Count == 0)
        {
            return command.CommandText;
        }

        var builder = new StringBuilder();
        foreach (DbParameter parameter in command.Parameters)
        {
            var value = parameter.Value;
            builder
                .Append(".param set ")
                .Append(parameter.ParameterName)
                .Append(' ')
                .AppendLine(
                    value == null || value == DBNull.Value
                        ? "NULL"
                        : _typeMapper.FindMapping(value.GetType())?.GenerateSqlLiteral(value)
                        ?? value.ToString());
        }

        return builder
            .AppendLine()
            .Append(command.CommandText).ToString();
    }
}
