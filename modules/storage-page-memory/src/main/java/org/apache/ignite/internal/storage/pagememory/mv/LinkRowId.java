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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.RowId;

/**
 * {@link RowId} implementation which identifies the row data using row link in page-memory.
 *
 * @see RowId
 * @see PageIdUtils#link(long, int)
 */
public class LinkRowId implements RowId {
    private final long rowLink;

    public LinkRowId(long rowLink) {
        this.rowLink = rowLink;
    }

    @Override
    public int partitionId() {
        long pageId = PageIdUtils.pageId(rowLink);
        return PageIdUtils.partitionId(pageId);
    }

    long versionChainLink() {
        return rowLink;
    }
}
