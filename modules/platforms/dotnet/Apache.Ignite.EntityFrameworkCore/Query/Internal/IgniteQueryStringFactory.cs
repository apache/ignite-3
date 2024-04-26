
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
