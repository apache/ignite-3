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

package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.exception.ExpectedReplicationException;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;

/**
 * Exception indicating that primary replica of sender partition has changed during write intent resolution requested by
 * read-write transaction. This means, that the requesting transaction won't be able to be committed, which is effectively equal
 * to the case of primary replica change during the handling of primary replica request, so this exception should cause
 * {@link PrimaryReplicaMissException} on the sender partition.
 */
public class PrimaryReplicaChangeDuringWriteIntentResolutionException extends IgniteInternalException
        implements ExpectedReplicationException {
    private static final long serialVersionUID = -8387939050713325533L;
}
