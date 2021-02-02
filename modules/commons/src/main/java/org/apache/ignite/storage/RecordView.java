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

package org.apache.ignite.storage;

/**
 * Record adapter for Table.
 */
public interface RecordView<T> {
    /**
     * Fills given record with the values from the table.
     *
     * @param keyRow Record with key fields set.
     * @return Record with all fields filled from the table.
     */
    public T get(T keyRow);

    /**
     * Insert new record into the table if it is not exists or replace existed one.
     *
     * @param row Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean upsert(T row);

    /**
     * Insert record into the table.
     *
     * @param row Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean insert(T row);
}
