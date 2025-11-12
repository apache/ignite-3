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
package org.apache.ignite.raft.jraft.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.util.Copiable;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.jetbrains.annotations.TestOnly;

/**
 * A configuration with a set of peers.
 */
public class Configuration implements Iterable<PeerId>, Copiable<Configuration> {

    private static final IgniteLogger LOG = Loggers.forClass(Configuration.class);

    private static final String LEARNER_POSTFIX = "/learner";

    public static final long NO_SEQUENCE_TOKEN = 0;

    private List<PeerId> peers = new ArrayList<>();

    // use LinkedHashSet to keep insertion order.
    private LinkedHashSet<PeerId> learners = new LinkedHashSet<>();

    private long sequenceToken;

    public Configuration() {
        super();
    }

    public Configuration(long sequenceToken) {
        this.sequenceToken = sequenceToken;
    }

    /**
     * Construct a configuration instance with peers.
     *
     * @param conf configuration
     */
    public Configuration(final Iterable<PeerId> conf, long sequenceToken) {
        this(conf, null, sequenceToken);
    }

    @TestOnly
    public Configuration(final Iterable<PeerId> conf) {
        this(conf, NO_SEQUENCE_TOKEN);
    }

    /**
     * Construct a configuration from another conf.
     *
     * @param conf configuration
     */
    public Configuration(final Configuration conf) {
        this(conf.getPeers(), conf.getLearners(), conf.getSequenceToken());
    }

    /**
     * Construct a configuration from another conf.
     *
     * @param conf configuration
     */
    public Configuration(final Configuration conf, long sequenceToken) {
        this(conf.getPeers(), conf.getLearners(), sequenceToken);
    }

    /**
     * Construct a Configuration instance with peers and learners.
     *
     * @param conf peers configuration
     * @param learners learners
     */
    public Configuration(final Iterable<PeerId> conf, final Iterable<PeerId> learners, long sequenceToken) {
        Requires.requireNonNull(conf, "conf");
        this.sequenceToken = sequenceToken;
        for (final PeerId peer : conf) {
            this.peers.add(peer.copy());
        }
        addLearners(learners);
    }

    @TestOnly
    public Configuration(final Iterable<PeerId> conf, final Iterable<PeerId> learners) {
       this(conf, learners, NO_SEQUENCE_TOKEN);
    }

    public void setLearners(final LinkedHashSet<PeerId> learners) {
        this.learners = learners;
    }

    /**
     * Add a learner peer.
     *
     * @param learner learner to add
     * @return true when add successfully.
     */
    public boolean addLearner(final PeerId learner) {
        return this.learners.add(learner);
    }

    /**
     * Add learners in batch, returns the added count.
     *
     * @param learners learners to add
     * @return the total added count
     */
    public int addLearners(final Iterable<PeerId> learners) {
        int ret = 0;
        if (learners != null) {
            for (final PeerId peer : learners) {
                if (this.learners.add(peer.copy())) {
                    ret++;
                }
            }
        }
        return ret;
    }

    /**
     * Remove a learner peer.
     *
     * @param learner learner to remove
     * @return true when remove successfully.
     */
    public boolean removeLearner(final PeerId learner) {
        return this.learners.remove(learner);
    }

    /**
     * Retrieve the learners set.
     *
     * @return learners
     */
    public LinkedHashSet<PeerId> getLearners() {
        return this.learners;
    }

    /**
    * A token that is bound to this configuration.
    */
    public long getSequenceToken() {
        return sequenceToken;
    }

    /**
     * Retrieve the learners set copy.
     *
     * @return learners
     */
    public List<PeerId> listLearners() {
        return new ArrayList<>(this.learners);
    }

    @Override
    public Configuration copy() {
        return new Configuration(this.peers, this.learners, this.sequenceToken);
    }

    /**
     * Returns true when the configuration is valid.
     *
     * @return true if the configuration is valid.
     */
    public boolean isValid() {
        final Set<PeerId> intersection = new HashSet<>(this.peers);
        intersection.retainAll(this.learners);
        return !this.peers.isEmpty() && intersection.isEmpty();
    }

    public void reset() {
        this.peers.clear();
        this.learners.clear();
    }

    public boolean isEmpty() {
        return this.peers.isEmpty();
    }

    /**
     * Returns the peers total number.
     *
     * @return total num of peers
     */
    public int size() {
        return this.peers.size();
    }

    @Override
    public Iterator<PeerId> iterator() {
        return this.peers.iterator();
    }

    public Set<PeerId> getPeerSet() {
        return new HashSet<>(this.peers);
    }

    public List<PeerId> listPeers() {
        return new ArrayList<>(this.peers);
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public void setPeers(final List<PeerId> peers) {
        this.peers.clear();
        for (final PeerId peer : peers) {
            this.peers.add(peer.copy());
        }
    }

    public void appendPeers(final Collection<PeerId> set) {
        this.peers.addAll(set);
    }

    public boolean addPeer(final PeerId peer) {
        return this.peers.add(peer);
    }

    public boolean removePeer(final PeerId peer) {
        return this.peers.remove(peer);
    }

    public boolean contains(final PeerId peer) {
        return this.peers.contains(peer);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.learners == null) ? 0 : this.learners.hashCode());
        result = prime * result + ((this.peers == null) ? 0 : this.peers.hashCode());
        result = prime * result + (int)sequenceToken;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Configuration other = (Configuration) obj;
        if(this.sequenceToken != other.sequenceToken) {
            return false;
        }
        if (this.learners == null) {
            if (other.learners != null) {
                return false;
            }
        }
        else if (!this.learners.equals(other.learners)) {
            return false;
        }
        if (this.peers == null) {
            return other.peers == null;
        }
        else {
            return this.peers.equals(other.peers);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final List<PeerId> peers = listPeers();
        int i = 0;
        int size = peers.size();
        for (final PeerId peer : peers) {
            sb.append(peer);
            if (i < size - 1 || !this.learners.isEmpty()) {
                sb.append(",");
            }
            i++;
        }

        size = this.learners.size();
        i = 0;
        for (final PeerId peer : this.learners) {
            sb.append(peer).append(LEARNER_POSTFIX);
            if (i < size - 1) {
                sb.append(",");
            }
            i++;
        }

        return sb.toString();
    }

    public boolean parse(final String conf) {
        if (StringUtils.isBlank(conf)) {
            return false;
        }
        reset();
        final String[] peerStrs = StringUtils.split(conf, ',');
        for (String peerStr : peerStrs) {
            final PeerId peer = new PeerId();
            int index;
            boolean isLearner = false;
            if ((index = peerStr.indexOf(LEARNER_POSTFIX)) > 0) {
                // It's a learner
                peerStr = peerStr.substring(0, index);
                isLearner = true;
            }
            if (peer.parse(peerStr)) {
                if (isLearner) {
                    addLearner(peer);
                }
                else {
                    addPeer(peer);
                }
            }
            else {
                LOG.error("Fail to parse peer {} in {}, ignore it.", peerStr, conf);
            }
        }
        return true;
    }

    /**
     * Get the difference between |*this| and |rhs| |included| would be assigned to |*this| - |rhs| |excluded| would be
     * assigned to |rhs| - |*this|
     */
    public void diff(final Configuration rhs, final Configuration included, final Configuration excluded) {
        included.peers = new ArrayList<>(this.peers);
        included.peers.removeAll(rhs.peers);
        excluded.peers = new ArrayList<>(rhs.peers);
        excluded.peers.removeAll(this.peers);
    }

    /**
     * Update the sequence token.
     */
    @TestOnly
    public void updateSequenceToken(long sequenceToken) {
        this.sequenceToken = sequenceToken;
    }
}
