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

package org.apache.ignite.internal.rest.deployment;

import io.micronaut.http.multipart.CompletedFileUpload;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Implementation of {@link Subscriber} based on {@link CompletedFileUpload} which will collect uploaded files to the
 * {@link DeploymentUnit}.
 */
class CompletedFileUploadSubscriber implements Subscriber<CompletedFileUpload>, AutoCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(CompletedFileUploadSubscriber.class);

    private final CompletableFuture<DeploymentUnit> result = new CompletableFuture<>();

    private final Map<String, InputStream> content = new HashMap<>();

    private IOException ex;

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(CompletedFileUpload item) {
        try {
            content.put(item.getFilename(), item.getInputStream());
        } catch (IOException e) {
            LOG.error("Failed to read file: " + item.getFilename(), e);
            if (ex != null) {
                ex.addSuppressed(e);
            } else {
                ex = e;
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        if (ex != null) {
            result.completeExceptionally(ex);
        } else {
            result.complete(new DeploymentUnit(content));
        }
    }

    public CompletableFuture<DeploymentUnit> result() {
        return result;
    }

    @Override
    public void close() throws Exception {
        result.thenAccept(it -> {
            try {
                it.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
