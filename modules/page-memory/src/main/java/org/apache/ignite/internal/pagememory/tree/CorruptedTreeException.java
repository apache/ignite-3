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

package org.apache.ignite.internal.pagememory.tree;

import java.util.Arrays;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.CorruptedDataStructureException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception to distinguish {@link BplusTree} tree broken invariants.
 */
public class CorruptedTreeException extends CorruptedDataStructureException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpName Group name.
     * @param grpId Group id.
     * @param pageIds PageId's that can be corrupted.
     */
    public CorruptedTreeException(String msg, @Nullable Throwable cause, String grpName, int grpId, long... pageIds) {
        this(msg, null, grpName, cause, grpId, pageIds);
    }

    /**
     * Constructor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpName Group name of potentially corrupted pages.
     * @param indexName Index name.
     * @param grpId Group id.
     * @param pageIds PageId's that can be corrupted.
     */
    public CorruptedTreeException(
            String msg,
            @Nullable Throwable cause,
            String grpName,
            String indexName,
            int grpId,
            long... pageIds
    ) {
        this(msg, indexName, grpName, cause, grpId, pageIds);
    }

    /**
     * Constructor.
     *
     * @param msg Message.
     * @param indexName Index name.
     * @param grpName Group name.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds PageId's that can be corrupted.
     */
    public CorruptedTreeException(
            String msg,
            String indexName,
            String grpName,
            @Nullable Throwable cause,
            int grpId,
            long... pageIds
    ) {
        super(getMsg(msg, indexName, grpName, grpId, pageIds), cause, grpId, pageIds);
    }

    /**
     * Creates {@link CorruptedTreeException} error message.
     *
     * @param msg Message.
     * @param indexName Index name.
     * @param grpName Group name.
     * @param grpId Group id.
     * @param pageIds PageId's that can be corrupted.
     * @return Error message.
     */
    private static String getMsg(
            String msg,
            @Nullable String indexName,
            @Nullable String grpName,
            int grpId,
            long... pageIds
    ) {
        IgniteStringBuilder sb = new IgniteStringBuilder("B+Tree is corrupted [groupId=");

        sb.app(grpId).app(", pageIds=").app(Arrays.toString(pageIds));

        if (indexName != null) {
            sb.app(", indexName=").app(indexName);
        }

        if (grpName != null) {
            sb.app(", groupName=").app(grpName);
        }

        sb.app(", msg=").app(msg).app(']');

        return sb.toString();
    }
}
