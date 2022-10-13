namespace Apache.Ignite.Internal.Table.Serialization;

using Ignite.Table;
using MessagePack;
using Proto;
using Proto.BinaryTuple;

/// <summary>
/// Serializer handler for <see cref="IIgniteTuple"/>.
/// </summary>
internal class TuplePairSerializerHandler : IRecordSerializerHandler<KvPair<IIgniteTuple, IIgniteTuple>>
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly TuplePairSerializerHandler Instance = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="TuplePairSerializerHandler"/> class.
    /// </summary>
    private TuplePairSerializerHandler()
    {
        // No-op.
    }

    /// <inheritdoc/>
    public KvPair<IIgniteTuple, IIgniteTuple> Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
    {
        // TODO: Deal with value nullability properly. Maybe use Tuple.Empty? Or keep it as is for perf?
        var columns = schema.Columns;
        var count = keyOnly ? schema.KeyColumnCount : columns.Count;
        var keyTuple = new IgniteTuple(count);
        var valTuple = keyOnly ? null! : new IgniteTuple(columns.Count - schema.KeyColumnCount);
        var tupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), count);

        for (var index = 0; index < count; index++)
        {
            var column = columns[index];

            var tuple = index < schema.KeyColumnCount ? keyTuple : valTuple;
            tuple[column.Name] = tupleReader.GetObject(index, column.Type, column.Scale);
        }

        return new(keyTuple, valTuple);
    }

    /// <inheritdoc/>
    public KvPair<IIgniteTuple, IIgniteTuple> ReadValuePart(ref MessagePackReader reader, Schema schema, KvPair<IIgniteTuple, IIgniteTuple> key)
    {
        var columns = schema.Columns;
        var tuple = new IgniteTuple(columns.Count);
        var tupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), schema.Columns.Count - schema.KeyColumnCount);

        for (var i = schema.KeyColumnCount; i < columns.Count; i++)
        {
            var column = columns[i];
            tuple[column.Name] = tupleReader.GetObject(i - schema.KeyColumnCount, column.Type, column.Scale);
        }

        return key with { Val = tuple };
    }

    /// <inheritdoc/>
    public void Write(ref MessagePackWriter writer, Schema schema, KvPair<IIgniteTuple, IIgniteTuple> record, bool keyOnly = false)
    {
        var columns = schema.Columns;
        var count = keyOnly ? schema.KeyColumnCount : columns.Count;
        var noValueSet = writer.WriteBitSet(count);

        var tupleBuilder = new BinaryTupleBuilder(count);

        try
        {
            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var colIdx = record.GetOrdinal(col.Name);

                if (colIdx >= 0)
                {
                    tupleBuilder.AppendObject(record[colIdx], col.Type, col.Scale);
                }
                else
                {
                    tupleBuilder.AppendNoValue(noValueSet);
                }
            }

            var binaryTupleMemory = tupleBuilder.Build();
            writer.Write(binaryTupleMemory.Span);
        }
        finally
        {
            tupleBuilder.Dispose();
        }
    }
}
