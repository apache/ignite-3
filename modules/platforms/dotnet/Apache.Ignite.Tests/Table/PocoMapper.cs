namespace Apache.Ignite.Tests.Table;

using System;
using Ignite.Table.Mapper;

public class PocoMapper : IMapper<Poco>
{
    public void Write(Poco obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        foreach (var col in schema.Columns)
        {
            switch (col.Name)
            {
                case "key":
                    rowWriter.WriteLong(obj.Key);
                    break;

                case "val":
                    rowWriter.WriteString(obj.Val);
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }
    }

    public Poco Read(ref RowReader rowReader, IMapperSchema schema)
    {
        var obj = new Poco();

        foreach (var col in schema.Columns)
        {
            switch (col.Name)
            {
                case "key":
                    obj.Key = rowReader.ReadLong()!.Value;
                    break;

                case "val":
                    obj.Val = rowReader.ReadString();
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }

        return obj;
    }
}
