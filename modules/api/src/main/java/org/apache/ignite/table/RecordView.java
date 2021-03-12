/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.table;

/**
 * Record adapter for Table.
 *
 * Note: Some methods expects a truncated record with key fields only defined can be used {@code <K>} as parameter,
 * and any value field will be ignored.
 *
 * @param <R> Record type.
 *
 * TODO: Record view is created with certain mapper.
 * TODO: Actually, it is enough to pass arbitrary object with key fields to "<K> R get(K keyRow)" method.
 * TODO: But, what mapper should we use? if user do not provide mapper for K class?
 * TODO: Normally, we cache mapper\serializer for R class.
 * TODO: Do we need an additional map "user Class->Serializer"?
 */
public interface RecordView<R> extends TableView<R> {
    /**
     * Fills given record with the values from the table.
     * Similar to {@link #get(Object)}, but return original object with filled value fields.
     *
     * Note: Value fields will be rewritten.
     *
     * @param recObjToFill Record object with key fields to be filled.
     * @return Record with all fields filled from the table.
     */
    public R fill(R recObjToFill);
}
