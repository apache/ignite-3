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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.persistence.PageCacheMetrics;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.Segment;

/** A factory that augments page replacement policies with metrics collection. */
public class MeteredPageReplacementPolicyFactory implements PageReplacementPolicyFactory {
    private final PageReplacementPolicyFactory delegate;
    private final PageCacheMetrics metrics;

    public MeteredPageReplacementPolicyFactory(PageCacheMetrics metrics, PageReplacementPolicyFactory delegate) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    public long requiredMemory(int pagesCnt) {
        return delegate.requiredMemory(pagesCnt);
    }

    @Override
    public PageReplacementPolicy create(Segment seg, long ptr, int pagesCnt) {
        return new MeteredPageReplacementPolicy(metrics, delegate.create(seg, ptr, pagesCnt));
    }

    private static class MeteredPageReplacementPolicy extends PageReplacementPolicy {
        private final PageReplacementPolicy delegate;
        private final PageCacheMetrics metrics;

        private MeteredPageReplacementPolicy(PageCacheMetrics metrics, PageReplacementPolicy delegate) {
            super(delegate.seg);
            this.metrics = metrics;
            this.delegate = delegate;
        }

        @Override
        public long replace() throws IgniteInternalCheckedException {
            metrics.incrementPageReplacement();
            return delegate.replace();
        }

        @Override
        public void onHit(long relPtr) {
            delegate.onHit(relPtr);
        }

        @Override
        public void onMiss(long relPtr) {
            metrics.incrementPageCacheMiss();
            delegate.onMiss(relPtr);
        }

        @Override
        public void onRemove(long relPtr) {
            delegate.onRemove(relPtr);
        }
    }
}
