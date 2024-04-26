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
