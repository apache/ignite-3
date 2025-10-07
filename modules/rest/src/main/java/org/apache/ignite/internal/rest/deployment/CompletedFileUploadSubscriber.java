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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorage;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Implementation of {@link Subscriber} based on {@link CompletedFileUpload} which will collect uploaded files to the
 * {@link DeploymentUnit}.
 */
class CompletedFileUploadSubscriber implements Subscriber<CompletedFileUpload> {
    private static final IgniteLogger LOG = Loggers.forClass(CompletedFileUploadSubscriber.class);

    private final CompletableFuture<DeploymentUnit> result = new CompletableFuture<>();

    private final InputStreamCollector collector;

    private Throwable ex;

    public CompletedFileUploadSubscriber(TempStorage tempStorage, boolean unzip) {
        this.collector = unzip
                ? new ZipInputStreamCollector(tempStorage)
                : new InputStreamCollectorImpl(tempStorage);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(CompletedFileUpload item) {
        String filename = item.getFilename();
        try {
            LOG.info("Receiving file: " + filename + ", size: " + item.getSize());
            InputStream is = item.getInputStream();
            LOG.info("Input stream obtained for file: " + filename + ", is: " + is);
            collector.addInputStream(filename, is);
            LOG.info("File added to collector: " + filename);
        } catch (IOException e) {
            LOG.error("Failed to read file: " + filename, e);
            suppressException(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("Subscriber failed: ", throwable);
        try {
            collector.rollback();
        } catch (Exception e) {
            suppressException(e);
        }
        suppressException(throwable);
        result.completeExceptionally(ex);
    }

    @Override
    public void onComplete() {
        LOG.info("Subscriber completed");
        if (ex != null) {
            result.completeExceptionally(ex);
        } else {
            try {
                DeploymentUnit deploymentUnit = collector.toDeploymentUnit();
                result.complete(deploymentUnit);
            } catch (Exception e) {
                suppressException(e);
                try {
                    collector.rollback();
                } catch (Exception e2) {
                    suppressException(e2);
                }
                result.completeExceptionally(ex);
            }
        }
    }

    private void suppressException(Throwable t) {
        LOG.warn("Deployment unit subscriber error: ", t);
        if (ex == null) {
            ex = t;
        } else {
            ex.addSuppressed(t);
        }
    }

    public CompletableFuture<DeploymentUnit> result() {
        return result;
    }
}
