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

package org.apache.ignite.client.compatibility.containers;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Container for an Ignite server instance.
 * This container uses the Apache Ignite 3.0.0 image.
 */
public class IgniteServerContainer implements Startable {
    private static final DockerImageName IGNITE_IMAGE = DockerImageName.parse("apacheignite/ignite");

    private final GenericContainer<?> container;

    public IgniteServerContainer(String tag) {
        this.container = createIgniteContainer(tag);
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createIgniteContainer(String tag) {
        Consumer<OutputFrame> logConsumer = outputFrame -> System.out.print(outputFrame.getUtf8String());

        return new GenericContainer<>(IGNITE_IMAGE.withTag(tag))
                .withExposedPorts(10300, 10800)
                .withLogConsumer(logConsumer)
                .waitingFor(Wait.forLogMessage(".*Joining the cluster.*?", 1));
    }

    @Override
    public void start() {
        Startables.deepStart(this.container).orTimeout(30, TimeUnit.SECONDS).join();
    }

    @Override
    public void stop() {
        container.stop();
    }

    public int clientPort() {
        return container.getMappedPort(10800);
    }

    public int restPort() {
        return container.getMappedPort(10300);
    }
}
