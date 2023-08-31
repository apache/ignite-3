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

package org.apache.ignite.internal.sql.engine.message;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.network.annotations.Transferable;

/**
 * QueryBatchMessage interface.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Transferable(value = SqlQueryMessageGroup.QUERY_BATCH_MESSAGE)
public interface QueryBatchMessage extends ExecutionContextAwareMessage {
    /**
     * Get exchange ID.
     */
    long exchangeId();

    /**
     * Get batch ID.
     */
    int batchId();

    /**
     * Get last batch flag.
     */
    boolean last();

    /**
     * Get rows.
     */
    List<ByteBuffer> rows();
}
