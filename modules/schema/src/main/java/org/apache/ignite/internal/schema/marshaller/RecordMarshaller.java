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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Record marshaller interface provides method to marshal/unmarshal record objects to/from a row.
 *
 * @param <R> Record type.
 */
public interface RecordMarshaller<R> {
    /**
     * Returns marshaller schema version.
     */
    int schemaVersion();

    /**
     * Marshals given record to a row.
     *
     * @param rec Record to marshal.
     * @return Table row with columns set from given object.
     * @throws MarshallerException If failed to marshal record.
     */
    Row marshal(R rec) throws MarshallerException;

    /**
     * Marshals key part of given record to a row.
     *
     * @param keyRec Record to marshal.
     * @return Table row with key columns set from given object.
     * @throws MarshallerException If failed to marshal record.
     */
    Row marshalKey(R keyRec) throws MarshallerException;

    /**
     * Unmarshal given row to a record object.
     *
     * @param row Table row.
     * @return Record object.
     * @throws MarshallerException If failed to unmarshal row.
     */
    R unmarshal(Row row) throws MarshallerException;

    /**
     * Reads object field value.
     *
     * @param obj    Object to read from.
     * @param fldIdx Field index.
     * @return Field value.
     */
    @Nullable Object value(Object obj, int fldIdx) throws MarshallerException;
}
