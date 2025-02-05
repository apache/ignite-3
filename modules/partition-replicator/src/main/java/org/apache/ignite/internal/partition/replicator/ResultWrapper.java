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

package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.raft.Command;

/**
 * Wrapper for the update(All)Command processing result that besides result itself stores actual command that was processed.
 */
public class ResultWrapper<T> {
    private final Command command;
    private final T result;

    public ResultWrapper(Command command, T result) {
        this.command = command;
        this.result = result;
    }

    public Command getCommand() {
        return command;
    }

    public T getResult() {
        return result;
    }
}
