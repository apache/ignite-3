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

package org.apache.ignite.internal.raft.service;

import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;

/**
 * Handles 'before apply': that is, before a RAFT command is accepted by a RAFT leader for processing,
 * executes some customization on the command.
 *
 * <p>For {@link WriteCommand}s, {@link #onBeforeApply(Command)} is executed atomically with accepting the command: that is,
 * before-apply/accept of one command cannot intermingle with before-apply/accept of other commands.
 *
 * <p>For {@link ReadCommand}s, no atomicity guarantees are provided.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface BeforeApplyHandler {
    /**
     * Invoked on a leader before submitting a command to a raft group.
     * If a command must be changed before saving to raft log,
     * this is a place to do it.
     *
     * @param command The command.
     * @return New if command has been changed, i.e. its fields have been modified. Same command as the argument otherwise.
     */
    Command onBeforeApply(Command command);
}
