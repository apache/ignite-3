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

package org.apache.ignite.sql;

import static java.util.stream.Collectors.toList;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Closeable cursor.
 *
 * @param <T> Type of elements.
 */
public interface ClosableCursor<T> extends Iterator<T>, AutoCloseable {
    /**
     * Returns a sequential Stream over the elements covered by this cursor.
     *
     * @return Sequential Stream over the elements covered by this cursor.
     */
    default Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false);
    }

    /**
     * Gets all query results and stores them in the collection.
     * Use this method when you know in advance that query result is
     * relatively small and will not cause memory utilization issues.
     * <p>Since all the results will be fetched, all the resources will be closed
     * automatically after this call, e.g. there is no need to call {@link #close()} method in this case.
     *
     * @return List containing all query results.
     */
    default List<T> getAll() {
        return stream().collect(toList());
    }

    /**
     * Closes the cursor also releasing all underlying resources.
     *
     * <p>This method should be implemented carefully; just wrapping a checked exception in an unchecked one
     * should not be used as the default option.
     */
    @Override
    void close();
}
