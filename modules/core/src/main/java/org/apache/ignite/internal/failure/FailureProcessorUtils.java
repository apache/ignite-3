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

package org.apache.ignite.internal.failure;

/**
 * Utils making it easier to report failures with {@link FailureProcessor}.
 */
public class FailureProcessorUtils {
    /**
     * Reports a failure to a failure processor capturing the call site context.
     *
     * @param processor Processor used to process the failure.
     * @param th The failure.
     * @param messageFormat Message format (same as {@link String#format(String, Object...)} accepts).
     * @param args Arguments for the message.
     */
    public static void processCriticalFailure(FailureProcessor processor, Throwable th, String messageFormat, Object... args) {
        processor.process(new FailureContext(th, String.format(messageFormat, args)));
    }
}
