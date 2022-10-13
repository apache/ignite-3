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
        var columns = schema.Columns;
        var count = keyOnly ? schema.KeyColumnCount : columns.Count;
        var tuple = new IgniteTuple(count);
        var tupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), count);

        for (var index = 0; index < count; index++)
        {
            var column = columns[index];
            tuple[column.Name] = tupleReader.GetObject(index, column.Type, column.Scale);
        }

        return tuple;
    }

    /// <inheritdoc/>
    public KvPair<IIgniteTuple, IIgniteTuple> ReadValuePart(ref MessagePackReader reader, Schema schema, KvPair<IIgniteTuple, IIgniteTuple> key)
    {
        var columns = schema.Columns;
        var tuple = new IgniteTuple(columns.Count);
        var tupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), schema.Columns.Count - schema.KeyColumnCount);

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];

            if (i < schema.KeyColumnCount)
            {
                tuple[column.Name] = key[column.Name];
            }
            else
            {
                tuple[column.Name] = tupleReader.GetObject(i - schema.KeyColumnCount, column.Type, column.Scale);
            }
        }

        return tuple;
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
