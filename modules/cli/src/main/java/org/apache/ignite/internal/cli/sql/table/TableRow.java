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

package org.apache.ignite.internal.cli.sql.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Data class for table row representation.
 *
 * @param <T> type of table row elements.
 */
public class TableRow<T> implements Iterable<T> {
    private final List<T> content;

    /**
     * Constructor.
     *
     * @param elements row elements.
     */
    public TableRow(Collection<T> elements) {
        content = new ArrayList<>(elements);
    }

    /**
     * Element getter.
     *
     * @param index in table row.
     * @return Element of table row with index {@param index}.
     */
    public T get(int index) {
        return content.get(index);
    }

    /**
     * Getter for table row elements.
     *
     * @return Unmodifiable collection of table row content.
     */
    public Collection<T> getValues() {
        return Collections.unmodifiableCollection(content);
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<T> iterator() {
        return content.iterator();
    }
}
