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

package org.apache.ignite.example.table;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;

public class MapperExample {
    static class CityIdConverter implements TypeConverter<String, Integer> {

        @Override
        public String  toObjectType(Integer columnValue) {
            return columnValue.toString();
        }

        @Override
        public Integer toColumnType(String cityId) {
            return Integer.parseInt(cityId);
        }
    }

    public static void main(String[] args) throws Exception {
        var mapper = Mapper.builder(Person.class)
                .automap()
                .map("cityId", "city_id", new CityIdConverter())
                .build();

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {
            RecordView<Person> view = client.tables()
                    .table("person")
                    .recordView(mapper);


            Person myPerson = new Person(2, "2", "John Doe", 40, "Apache");

            view.upsert(null, myPerson);
        }
    }
}
