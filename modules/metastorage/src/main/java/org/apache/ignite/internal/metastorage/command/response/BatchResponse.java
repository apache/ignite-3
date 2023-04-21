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

package org.apache.ignite.internal.metastorage.command.response;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.PaginationCommand;

/**
 * Response for a {@link PaginationCommand}, containing the requested batch.
 */
public class BatchResponse implements Serializable {
    private static final long serialVersionUID = 8679671467542166801L;

    private final List<Entry> entries;

    private final boolean hasNextBatch;

    /**
     * Constructor.
     *
     * @param entries Entries that comprise this batch.
     * @param hasNextBatch Flag indicating whether the remote cursor has more data in it.
     */
    public BatchResponse(List<Entry> entries, boolean hasNextBatch) {
        this.entries = entries;
        this.hasNextBatch = hasNextBatch;
    }

    public List<Entry> entries() {
        return entries;
    }

    public boolean hasNextBatch() {
        return hasNextBatch;
    }
}
