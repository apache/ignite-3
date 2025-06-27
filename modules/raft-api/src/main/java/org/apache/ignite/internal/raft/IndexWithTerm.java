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

package org.apache.ignite.internal.raft;

/**
 * Raft index with the corresponding term.
 */
public class IndexWithTerm implements Comparable<IndexWithTerm> {
    private final long index;
    private final long term;

    /** Constructor. */
    public IndexWithTerm(long index, long term) {
        this.index = index;
        this.term = term;
    }

    /** Returns the index. */
    public long index() {
        return index;
    }

    /** Returns the term corresponding to the index. */
    public long term() {
        return term;
    }

    @Override
    public int compareTo(IndexWithTerm that) {
        int byTerm = Long.compare(this.term, that.term);
        if (byTerm != 0) {
            return byTerm;
        }

        return Long.compare(this.index, that.index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexWithTerm that = (IndexWithTerm) o;

        if (index != that.index) {
            return false;
        }
        return term == that.term;
    }

    @Override
    public int hashCode() {
        int result = (int) (index ^ (index >>> 32));
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return index + ":" + term;
    }
}
