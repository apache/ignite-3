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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;

/**
 * MailboxRegistry interface.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public interface MailboxRegistry extends LifecycleAware {
    /**
     * Registers an inbox.
     *
     * @param inbox Inbox to register.
     */
    void register(Inbox<?> inbox);

    /**
     * Registers an outbox.
     *
     * @param outbox Outbox to register.
     */
    void register(Outbox<?> outbox);

    /**
     * Unregisters an inbox.
     *
     * @param inbox Inbox to unregister.
     */
    void unregister(Inbox<?> inbox);

    /**
     * Unregisters an outbox.
     *
     * @param outbox Outbox to unregister.
     */
    void unregister(Outbox<?> outbox);

    /**
     * Returns a registered outbox by provided query ID, exchange ID pair.
     *
     * @param qryId      Query ID.
     * @param exchangeId Exchange ID.
     * @return Registered outbox. May be {@code null} if execution was cancelled.
     */
    CompletableFuture<Outbox<?>> outbox(UUID qryId, long exchangeId);

    /**
     * Returns a registered inbox by provided query ID, exchange ID pair.
     *
     * @param qryId      Query ID.
     * @param exchangeId Exchange ID.
     * @return Registered inbox. May be {@code null} if execution was cancelled.
     */
    Inbox<?> inbox(UUID qryId, long exchangeId);
}
