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

namespace Apache.Ignite.Tests.Table.Serialization;

using Ignite.Table.Mapper;
using Internal.Table.Serialization;

/// <summary>
/// Tests for <see cref="MapperSerializerHandler{T}"/>.
/// </summary>
internal class MapperSerializerHandlerTests : SerializerHandlerTestBase
{
    protected override IRecordSerializerHandler<T> GetHandler<T>()
    {
        return new MapperSerializerHandler<T>((IMapper<T>)new PocoMapper());
    }

    public class PocoMapper : IMapper<Poco>
    {
        public void Write(Poco obj, ref MapperWriter writer, IMapperSchema schema)
        {
            // TODO
        }

        public Poco Read(ref MapperReader reader, IMapperSchema schema)
        {
            return new Poco();
        }
    }
}
