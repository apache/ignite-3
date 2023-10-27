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

package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.exception.ExpectedReplicationException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;

/**
 * An exception that is thrown to indicate that schema version of {@link BinaryRow}(s) passed in a {@link ReplicaRequest} do not match
 * the schema version corresponding to the transaction. This can only happen if the caller first obtained the schema
 * and then started an (implicit) transaction; in such a case, the caller should retry the sequence (that is,
 * re-obtain the new schema version, start another implicit transaction and send new ReplicaRequest).
 *
 * <p>This exception should never reach the public API users (even as a cause or a suppressed exception),
 * that's why it has no error code attached.
 */
public class InternalSchemaVersionMismatchException extends IgniteInternalException implements ExpectedReplicationException {
    private static final long serialVersionUID = 7695731405107811859L;
}
