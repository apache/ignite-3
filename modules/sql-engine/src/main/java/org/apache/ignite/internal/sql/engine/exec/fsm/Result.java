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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Result of evaluation of particular {@link ExecutionPhase}. */
class Result {
    enum Status {
        PROCEED_IMMEDIATELY, SCHEDULE, STOP
    }

    private final Status status;

    @IgniteToStringExclude
    private final @Nullable CompletableFuture<Void> await;

    static Result proceedImmediately() {
        return new Result(Status.PROCEED_IMMEDIATELY, null);
    }

    static Result proceedAfter(CompletableFuture<Void> stage) {
        assert stage != null;

        return new Result(Status.SCHEDULE, stage);
    }

    static Result stop() {
        return new Result(Status.STOP, null);
    }

    private Result(Status status, @Nullable CompletableFuture<Void> await) {
        this.status = status;
        this.await = await;
    }

    Status status() {
        return status;
    }

    @Nullable CompletableFuture<Void> await() {
        return await;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
