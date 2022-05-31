/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Mailbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.Nullable;

/**
 * MailboxRegistryImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MailboxRegistryImpl implements MailboxRegistry, TopologyEventHandler {
    private static final IgniteLogger LOG = IgniteLogger.forClass(MailboxRegistryImpl.class);

    private static final Predicate<Mailbox<?>> ALWAYS_TRUE = o -> true;

    private final Map<MailboxKey, Outbox<?>> locals;

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
    public <T> Inbox<T> register(Inbox<T> inbox) {
        Inbox<T> old = (Inbox<T>) remotes.putIfAbsent(new MailboxKey(inbox.queryId(), inbox.exchangeId()), inbox);

        if (LOG.isTraceEnabled()) {
            if (old != null) {
                LOG.trace("Inbox already registered [qryId={}, fragmentId={}]", inbox.queryId(), inbox.fragmentId());
            } else {
                LOG.trace("Inbox registered [qryId={}, fragmentId={}]", inbox.queryId(), inbox.fragmentId());
            }
        }

        return old != null ? old : inbox;
    }

    /** {@inheritDoc} */
    @Override
    public void register(Outbox<?> outbox) {
        Outbox<?> res = locals.put(new MailboxKey(outbox.queryId(), outbox.exchangeId()), outbox);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Outbox registered [qryId={}, fragmentId={}]", outbox.queryId(), outbox.fragmentId());
        }

        assert res == null : res;
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
        boolean removed = locals.remove(new MailboxKey(outbox.queryId(), outbox.exchangeId()), outbox);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Outbox {} unregistered [qryId={}, fragmentId={}]", removed ? "was" : "wasn't",
                    outbox.queryId(), outbox.fragmentId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public Outbox<?> outbox(UUID qryId, long exchangeId) {
        return locals.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override
    public Inbox<?> inbox(UUID qryId, long exchangeId) {
        return remotes.get(new MailboxKey(qryId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Inbox<?>> inboxes(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        return remotes.values().stream()
                .filter(makeFilter(qryId, fragmentId, exchangeId))
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Outbox<?>> outboxes(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        return locals.values().stream()
                .filter(makeFilter(qryId, fragmentId, exchangeId))
                .collect(Collectors.toList());
    }

    private static Predicate<Mailbox<?>> makeFilter(@Nullable UUID qryId, long fragmentId, long exchangeId) {
        Predicate<Mailbox<?>> filter = ALWAYS_TRUE;
        if (qryId != null) {
            filter = filter.and(mailbox -> Objects.equals(mailbox.queryId(), qryId));
        }
        if (fragmentId != -1) {
            filter = filter.and(mailbox -> mailbox.fragmentId() == fragmentId);
        }
        if (exchangeId != -1) {
            filter = filter.and(mailbox -> mailbox.exchangeId() == exchangeId);
        }

        return filter;
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
        locals.values().forEach(n -> n.onNodeLeft(member.id()));
        remotes.values().forEach(n -> n.onNodeLeft(member.id()));
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
