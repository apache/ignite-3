namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteRelationalCommandBuilder : IRelationalCommandBuilder
{
    private readonly List<IRelationalParameter> _parameters = new List<IRelationalParameter>();
    private readonly IndentedStringBuilder _commandTextBuilder = new IndentedStringBuilder();

    public IgniteRelationalCommandBuilder(RelationalCommandBuilderDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual RelationalCommandBuilderDependencies Dependencies { get; }

    [Obsolete("Code trying to add parameter should add type mapped parameter using TypeMappingSource directly.")]
    public virtual IRelationalTypeMappingSource TypeMappingSource => Dependencies.TypeMappingSource;

    public virtual IRelationalCommand Build()
    {
        return new IgniteRelationalCommand(Dependencies, _commandTextBuilder.ToString(), Parameters);
    }

    public override string ToString() => _commandTextBuilder.ToString();

    /// <inheritdoc />
    public virtual IReadOnlyList<IRelationalParameter> Parameters
    {
        get => _parameters;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder AddParameter(IRelationalParameter parameter)
    {
        _parameters.Add(parameter);
        return this;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder RemoveParameterAt(int index)
    {
        _parameters.RemoveAt(index);
        return this;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder Append(string value)
    {
        _commandTextBuilder.Append(value);
        return this;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder AppendLine()
    {
        _commandTextBuilder.AppendLine();
        return this;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder IncrementIndent()
    {
        _commandTextBuilder.IncrementIndent();
        return this;
    }

    /// <inheritdoc />
    public virtual IRelationalCommandBuilder DecrementIndent()
    {
        _commandTextBuilder.DecrementIndent();
        return this;
    }

    /// <inheritdoc />
    public virtual int CommandTextLength => _commandTextBuilder.Length;
}
