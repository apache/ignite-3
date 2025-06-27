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

package org.apache.ignite.internal.metrics.exporters;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for push metrics exporters, according to terminology from {@link MetricExporter} docs.
 * Every {@code period} of time {@link PushMetricExporter#report()} will be called
 * to push metrics to the external system.
 */
public abstract class PushMetricExporter<CfgT extends ExporterView> extends BasicMetricExporter<CfgT> {
    /** Logger. */
    protected final IgniteLogger log = Loggers.forClass(getClass());

    /** Export task future. */
    private @Nullable ScheduledFuture<?> fut;

    /** Export scheduler. */
    private ScheduledExecutorService scheduler;

    private long period;

    @Override
    public synchronized void start(MetricProvider metricProvider, CfgT conf, Supplier<UUID> clusterIdSupplier, String nodeName) {
        super.start(metricProvider, conf, clusterIdSupplier, nodeName);

        scheduler = newSingleThreadScheduledExecutor(IgniteThreadFactory.create(nodeName, "metrics-exporter-" + name(), log));

        reconfigure(conf);
    }

    @Override
    public synchronized void reconfigure(CfgT newVal) {
        super.reconfigure(newVal);

        long newPeriod = period();

        if (fut == null || period != newPeriod) {
            if (fut != null) {
                fut.cancel(false);
            }

            period = newPeriod;

            fut = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    report();
                } catch (Throwable th) {
                    log.error("Metrics export error. This exporter will be stopped [class=" + getClass() + ",name=" + name() + ']', th);

                    throw th;
                }
            }, newPeriod, newPeriod, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void stop() {
        if (fut != null) {
            fut.cancel(false);

            fut = null;
        }

        if (scheduler != null) {
            IgniteUtils.shutdownAndAwaitTermination(scheduler, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Returns export period.
     *
     * @return Period in milliseconds after {@link #report()} method should be called.
     */
    protected abstract long period();

    /**
     * A heart of the push exporter.
     * Inside this method all needed operations to send the metrics outside must be implemented.
     *
     * <p>This method will be executed periodically by internal exporter's scheduler.
     *
     * <p>In case of any exceptions exporter's internal scheduler will be stopped
     * and no new {@code report} will be executed.
     */
    public abstract void report();
}
