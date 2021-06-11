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

package org.apache.ignite.storage.api;

import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;

/**
 * Interface providing methods to read, remove and update keys in storage.
 */
public interface Storage {
    /**
     * Reads a DataRow for a given Key.
     */
    public DataRow read(SearchRow key) throws StorageException;

    /**
     * Removes DataRow associated with a given Key.
     */
    public void remove(SearchRow key) throws StorageException;

    /** */
    public void write(DataRow row) throws StorageException;

    /**
     * Executes an update with custom logic implemented by storage.UpdateClosure interface.
     */
    public void invoke(DataRow key, InvokeClosure clo) throws StorageException;

    /** */
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException;
}

