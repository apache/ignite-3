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

package org.apache.ignite.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Task that reads provided input stream line by line and supplies these lines
 * to specified consumer.
 */
class StreamGrabberTask implements Runnable {
    private static final IgniteLogger LOG = Loggers.forClass(StreamGrabberTask.class);

    private final InputStream streamToGrab;

    private final Consumer<String> lineConsumer;

    StreamGrabberTask(InputStream streamToGrab, Consumer<String> lineConsumer) {
        this.streamToGrab = streamToGrab;
        this.lineConsumer = lineConsumer;
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(streamToGrab));

            String line;

            // noinspection NestedAssignment
            while (!Thread.currentThread().isInterrupted() && (line = br.readLine()) != null) {
                lineConsumer.accept(line);
            }
        } catch (IOException e) {
            LOG.error("Caught IOException while grabbing stream", e);
        }
    }
}
