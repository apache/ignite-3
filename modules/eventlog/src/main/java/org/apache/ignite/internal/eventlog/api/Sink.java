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

package org.apache.ignite.internal.eventlog.api;

/**
 * The endpoint for the event log framework. This is the last step in the event log pipeline. It can be a log file, a webhook, a Kafka
 * topic, or whatever we develop.
 *
 * <p>The contract of the only method is the following:
 *
 * <p>IT DOES NOT GUARANTEE THAT THE EVENT IS WRITTEN TO THE FINAL DESTINATION.
 * For example, if the sink as a log file, the method does not guarantee that the event is written to the file. Because the logging
 * framework can be asynchronous.
 *
 * <p>IT DOES GUARANTEE THAT THE EVENT IS SENT TO THE SINK.
 * For example, if the sink is a Kafka topic, the method guarantees that the event is sent to the topic.
 */
public interface Sink {
    /**
     * Writes the event to the sink.
     *
     * @param event The event to write.
     */
    void write(Event event);
}
