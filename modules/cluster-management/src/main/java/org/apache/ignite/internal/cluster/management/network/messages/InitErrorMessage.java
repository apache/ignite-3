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

package org.apache.ignite.internal.cluster.management.network.messages;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Message that represents an error condition that has occurred during cluster initialization.
 */
@Transferable(CmgMessageGroup.INIT_ERROR)
public interface InitErrorMessage extends NetworkMessage {
    /**
     * Text representation of the occurred error.
     */
    String cause();

    /**
     * Flag declaring that the cause of this error is an internal message, in which case the init procedure should be cancelled.
     * If {@code false} - this means that the error is caused by invalid user input and the command should be retried.
     */
    boolean shouldCancel();
}
