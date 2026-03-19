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

package org.apache.ignite.raft.jraft.option;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.raft.jraft.error.RaftError;

/**
 * Allows to implement validation for safe time in Raft. If a command's safe time is invalid, it gets rejected just before being added
 * to the leader's log (and hence does not get replicated). A rejected command can either be rejected for retry (and then the Raft client
 * should try again later) or for good (in which case the Raft client should not retry the operation and just communicate the error
 * to the caller instead).
 */
public interface SafeTimeValidator {
    /** Whether to validate safe time for the given command. */
    boolean shouldValidateFor(WriteCommand command);

    /** Whether the given safe time is valid for the given command. */
    SafeTimeValidationResult validate(String groupId, WriteCommand command, HybridTimestamp safeTime);
}
