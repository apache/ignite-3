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

package org.apache.ignite.internal.metastorage.common.command;

import java.util.List;
import org.apache.ignite.internal.metastorage.common.OperationInfo;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * Represents invoke command for meta storage.
 */
@Transferable(MetastorageCommandsMessageGroup.INVOKE)
public interface InvokeCommand extends NetworkMessage, WriteCommand {
    /**
     * Returns condition.
     *
     * @return Condition.
     */
    ConditionInfo condition();

    /**
     * Returns success operations.
     *
     * @return Success operations.
     */
    List<OperationInfo> success();

    /**
     * Returns failure operations.
     *
     * @return Failure operations.
     */
    List<OperationInfo> failure();
}
