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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;

/**
 * MailboxRegistryImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MailboxRegistryImpl implements MailboxRegistry, TopologyEventHandler {
    private static final IgniteLogger LOG = Loggers.forClass(MailboxRegistryImpl.class);

    private final Map<MailboxKey, CompletableFuture<Outbox<?>>> locals;

    private final Map<MailboxKey, Inbox<?>> remotes;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public MailboxRegistryImpl() {
        locals = new ConcurrentHashMap<>();
        remotes = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override
    public void register(Inbox<?> inbox) {
        Inbox<?> res = remotes.putIfAbsent(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Inbox registered [qryId={}, fragmentId={}]", inbox.queryId(), inbox.fragmentId());
        }

        assert res == null : res;
    }

    /** {@inheritDoc} */
    @Override
    public void register(Outbox<?> outbox) {
        CompletableFuture<Outbox<?>> res = locals.computeIfAbsent(new MailboxKey(outbox.queryId(), outbox.exchangeId()),
                k -> new CompletableFuture<>());

        assert !res.isDone();

        res.complete(outbox);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Outbox registered [qryId={}, fragmentId={}]", outbox.queryId(), outbox.fragmentId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void unregister(Inbox<?> inbox) {
        boolean removed = remotes.remove(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Inbox {} unregistered [qryId={}, fragmentId={}]", removed ? "was" : "wasn't",
                    inbox.queryId(), inbox.fragmentId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void unregister(Outbox<?> outbox) {
        boolean removed = locals.remove(new MailboxKey(outbox.queryId(), outbox.exchangeId())) != null;

        if (LOG.isTraceEnabled()) {
            LOG.trace("Outbox {} unregistered [qryId={}, fragmentId={}]", removed ? "was" : "wasn't",
                    outbox.queryId(), outbox.fragmentId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Outbox<?>> outbox(UUID qryId, long exchangeId) {
        return locals.computeIfAbsent(new MailboxKey(qryId, exchangeId), k -> new CompletableFuture<>());
    }

    /** {@inheritDoc} */
    @Override
    public Inbox<?> inbox(UUID qryId, long exchangeId) {
        return remotes.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(MailboxRegistryImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        locals.clear();
        remotes.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void onAppeared(ClusterNode member) {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override
    public void onDisappeared(ClusterNode member) {
        locals.values().forEach(fut -> fut.thenAccept(n -> n.onNodeLeft(member.name())));
        remotes.values().forEach(n -> n.onNodeLeft(member.name()));
    }

    private static class MailboxKey {
        private final UUID qryId;

        private final long exchangeId;

        private MailboxKey(UUID qryId, long exchangeId) {
            this.qryId = qryId;
            this.exchangeId = exchangeId;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MailboxKey that = (MailboxKey) o;

            if (exchangeId != that.exchangeId) {
                return false;
            }
            return qryId.equals(that.qryId);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            int res = qryId.hashCode();
            res = 31 * res + (int) (exchangeId ^ (exchangeId >>> 32));
            return res;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(MailboxKey.class, this);
        }
    }
}
