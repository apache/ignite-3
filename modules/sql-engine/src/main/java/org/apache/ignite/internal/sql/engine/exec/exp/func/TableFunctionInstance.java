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

package org.apache.ignite.internal.sql.engine.exec.exp.func;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Represents an instance of table function.
 *
 * @param <RowT> Row type.
 */
public interface TableFunctionInstance<RowT> extends Iterator<RowT>, AutoCloseable {

    /** Returns a table function that always produces no results. */
    static <RowT> TableFunctionInstance<RowT> empty() {
        return new TableFunctionInstance<>() {
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public RowT next() {
                throw new NoSuchElementException();
            }
        };
    }
}
