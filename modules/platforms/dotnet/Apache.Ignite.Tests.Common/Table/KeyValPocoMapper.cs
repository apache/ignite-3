/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests.Common.Table;

using System.Collections.Generic;
using Apache.Ignite.Internal.Common;
using Apache.Ignite.Table.Mapper;

public class KeyValPocoMapper : IMapper<KeyValuePair<KeyPoco, ValPoco>>
{
    public void Write(KeyValuePair<KeyPoco, ValPoco> obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        rowWriter.WriteLong(obj.Key.Key);

        if (schema.Columns.Count > 1)
        {
            IgniteArgumentCheck.NotNull(obj.Value, "val");

            rowWriter.WriteString(obj.Value.Val);
        }
    }

    public KeyValuePair<KeyPoco, ValPoco> Read(ref RowReader rowReader, IMapperSchema schema)
    {
        var key = new KeyPoco
        {
            Key = rowReader.ReadLong()!.Value
        };

        var val = schema.Columns.Count > 1
            ? new ValPoco
            {
                Val = rowReader.ReadString()
            }
            : null;

        return new KeyValuePair<KeyPoco, ValPoco>(key, val!);
    }
}
