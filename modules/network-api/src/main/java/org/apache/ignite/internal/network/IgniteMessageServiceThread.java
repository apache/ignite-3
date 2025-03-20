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

package org.apache.ignite.internal.network;

import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.ThreadOperation;

/**
 * Extension for use in {@link MessagingService} implementations that will allow, for example, tracking long message processing, which can
 * be critical for network communications.
 */
public class IgniteMessageServiceThread extends IgniteThread {
    /**
     * Creates thread with given name.
     *
     * @param finalName Name of thread.
     * @param r Runnable to execute.
     * @param allowedOperations Operations which this thread allows to execute.
     */
    public IgniteMessageServiceThread(String finalName, Runnable r, ThreadOperation... allowedOperations) {
        super(finalName, r, allowedOperations);
    }
}
