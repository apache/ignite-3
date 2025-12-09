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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.wrapper.Wrappers;

/** A job that writes values specified in constants to node's vault. */
public class PutVaultEntriesJob implements ComputeJob<String, Void> {
    public static final ByteArray TEST_KEY = new ByteArray("test_key".getBytes(StandardCharsets.UTF_8));

    public static final byte[] TEST_VALUE = "test_value".getBytes(StandardCharsets.UTF_8);

    public static final ByteArray OVERWRITTEN_KEY = new ByteArray("overwritten_key".getBytes(StandardCharsets.UTF_8));

    public static final byte[] INITIAL_VALUE = "initial_value".getBytes(StandardCharsets.UTF_8);

    public static final byte[] NEW_VALUE = "NEW_VALUE".getBytes(StandardCharsets.UTF_8);

    public static final ByteArray REMOVED_KEY = new ByteArray("removed_key".getBytes(StandardCharsets.UTF_8));

    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, String arg) {

        try {
            IgniteImpl igniteImpl = Wrappers.unwrap(context.ignite(), IgniteImpl.class);

            VaultManager vault = igniteImpl.vault();

            vault.put(TEST_KEY, TEST_VALUE);
            vault.put(OVERWRITTEN_KEY, INITIAL_VALUE);
            vault.put(REMOVED_KEY, INITIAL_VALUE);
            vault.put(OVERWRITTEN_KEY, NEW_VALUE);
            vault.remove(REMOVED_KEY);

            return nullCompletedFuture();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
