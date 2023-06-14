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

package org.apache.ignite.internal.cli.core.call;

import java.util.concurrent.atomic.AtomicLong;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

/** {@link ProgressBar} based tracker. */
public class ProgressBarTracker implements ProgressTracker {
    private final ProgressBarBuilder progressBarBuilder;

    private final AtomicLong maxSize = new AtomicLong(0);

    private volatile ProgressBar progressBar;

    ProgressBarTracker(ProgressBarBuilder progressBarBuilder) {
        this.progressBarBuilder = progressBarBuilder;
    }

    @Override
    public synchronized void track(long size) {
        ensureProgressBar();
        progressBar.stepTo(size);
    }

    @Override
    public void maxSize(long size) {
        ensureProgressBar();
        maxSize.compareAndSet(0, size);
        progressBar.maxHint(size);
    }

    @Override
    public void done() {
        ensureProgressBar();
        progressBar.stepTo(maxSize.get());
    }

    @Override
    public void close() {
        if (progressBar != null) {
            progressBar.close();
        }
    }

    private void ensureProgressBar() {
        if (progressBar == null) {
            progressBar = progressBarBuilder.build();
        }
    }
}
