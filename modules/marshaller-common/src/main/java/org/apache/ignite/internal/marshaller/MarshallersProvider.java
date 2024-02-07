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

package org.apache.ignite.internal.marshaller;

import org.apache.ignite.table.mapper.Mapper;

/** Provides marshaller instances. */
public interface MarshallersProvider {
    /**
     * Returns a marshaller for key columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    Marshaller getKeysMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    );

    /**
     * Returns a marshaller for value columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    Marshaller getValuesMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    );

    /**
     * Returns a marshaller that includes both key and value columns of the given schema.
     *
     * @param schema Schema.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    Marshaller getRowMarshaller(
            MarshallerSchema schema,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    );

    /**
     * Returns a marshaller for the given columns.
     *
     * @param columns Columns.
     * @param mapper Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @param allowUnmappedFields Whether specified class can contain fields that are not mapped to columns.
     * @return Marshaller.
     */
    Marshaller getMarshaller(
            MarshallerColumn[] columns,
            Mapper<?> mapper,
            boolean requireAllFields,
            boolean allowUnmappedFields
    );
}
