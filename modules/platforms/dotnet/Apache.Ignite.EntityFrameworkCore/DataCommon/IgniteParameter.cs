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
using System.Data;
using System.Data.Common;
using Apache.Ignite.Sql;

public class IgniteParameter : DbParameter
{
    public IgniteParameter()
    {
    }

    public override void ResetDbType()
    {
        throw new NotImplementedException();
    }

    public ColumnType ColumnType { get; set; }

    public override DbType DbType { get; set; }

    public override ParameterDirection Direction { get; set; }

    public override bool IsNullable { get; set; }

    public override string ParameterName { get; set; }

    public override string SourceColumn { get; set; }

    public override object Value { get; set; }

    public override bool SourceColumnNullMapping { get; set; }

    public override int Size { get; set; }
}
