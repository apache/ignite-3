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

package org.apache.ignite.table.mapper;

/**
 * Type converter interface provides methods for additional data transformation of the field type to a type 
 * compatible with the column type, and vice versa.
 *
 * <p>The converter can be used to convert objects (or their fields) whose type is incompatible with the schema.
 * E.g., serialize an arbitrary object to a byte[] for storing is a BLOB column.
 *
 * @param <ObjectT> Object type.
 * @param <ColumnT> Column type.
 */
public interface TypeConverter<ObjectT, ColumnT> {
    /**
     * Converts a given object to a column type.
     *
     * @param obj Object to transform.
     * @return Object of column type.
     * @throws Exception If transformation failed.
     */
    ColumnT toColumnType(ObjectT obj) throws Exception;

    /**
     * Transforms to an object of the target type; called after the data is read from a column.
     *
     * @param data Column data.
     * @return Object of the target type.
     * @throws Exception If transformation failed.
     */
    ObjectT toObjectType(ColumnT data) throws Exception;
}
