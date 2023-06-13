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

package org.apache.ignite.internal.cli.call.cluster.unit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.rest.client.invoker.ApiCallback;
import org.apache.ignite.rest.client.invoker.ApiException;

/** API callback that tracks progress. */
public class TrackingCallback<T> implements ApiCallback<T> {

    private final CountDownLatch latch;
    private final ProgressTracker tracker;

    private Exception exception;

    TrackingCallback(ProgressTracker tracker) {
        this.latch = new CountDownLatch(1);
        this.tracker = tracker;
    }

    public void awaitDone() throws InterruptedException {
        latch.await();
    }

    public Exception exception() {
        return exception;
    }

    @Override
    public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
        exception = e;
        latch.countDown();
    }

    @Override
    public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
        latch.countDown();
    }

    @Override
    public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
        if (done) {
            tracker.done();
        }
        tracker.maxSize(contentLength);
        tracker.track(bytesWritten);
    }

    @Override
    public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
        // No-op.
    }
}
