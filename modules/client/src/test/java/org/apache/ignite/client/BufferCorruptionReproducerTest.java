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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Test to reproduce buffer corruption in splitAndRun due to premature buffer release.
 *
 * Issue: When getAllAsync splits requests across multiple partitions, the PayloadInputChannel
 * closes immediately after reading, releasing pooled ByteBuffers before the application
 * consumes the lazy BinaryTupleReader instances. This causes data corruption.
 */
public class BufferCorruptionReproducerTest extends AbstractClientTableTest {

    /**
     * Reproduces buffer corruption by:
     * 1. Inserting many records that will be distributed across multiple partitions
     * 2. Calling getAllAsync which uses splitAndRun
     * 3. Forcing a delay between buffer release and data access
     * 4. Verifying that data is corrupted or missing
     */
    @Test
    public void testGetAllBufferCorruptionWithDelay() throws Exception {
        Table table = defaultTable();
        RecordView<Tuple> view = table.recordView();

        // Insert test data - enough to span multiple partitions
        List<Tuple> insertData = new ArrayList<>();
        Map<Long, String> expectedValues = new HashMap<>();

        for (long i = 1; i <= 100; i++) {
            String name = "Person_" + i;
            insertData.add(tuple(i, name));
            expectedValues.put(i, name);
        }

        view.upsertAll(null, insertData);

        // Prepare keys for getAll
        List<Tuple> keys = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            keys.add(tupleKey(i));
        }

        // Call getAllAsync - this will trigger splitAndRun with multiple partition batches
        CompletableFuture<List<Tuple>> future = view.getAllAsync(null, keys);

        // CRITICAL: Add delay to allow buffers to be released and potentially reused
        // This simulates real-world conditions where the application doesn't immediately
        // consume the results
        Thread.sleep(100);

        // Try to trigger buffer pool reuse by creating more traffic
        for (int j = 0; j < 10; j++) {
            view.upsertAsync(null, tuple(200L + j, "ExtraData_" + j));
        }

        // Force GC to increase chances of buffer corruption
        System.gc();
        Thread.sleep(50);

        // Now get the results - the buffers may have been reused!
        List<Tuple> results = future.get(5, TimeUnit.SECONDS);

        // Verify results
        assertEquals(100, results.size(), "Should get all 100 records");

        int corruptionCount = 0;
        int nullCount = 0;

        for (Tuple result : results) {
            assertNotNull(result, "Result should not be null");

            Long id = result.longValue("id");
            String name = result.stringValue("name");

            if (id == null) {
                nullCount++;
                continue;
            }

            String expectedName = expectedValues.get(id);

            if (expectedName == null || !expectedName.equals(name)) {
                corruptionCount++;
                System.err.println("CORRUPTION DETECTED: Expected id=" + id
                    + " to have name='" + expectedName + "', but got '" + name + "'");
            }
        }

        // The test will fail if corruption is detected
        assertEquals(0, corruptionCount,
            "Detected " + corruptionCount + " corrupted records due to buffer reuse");
        assertEquals(0, nullCount,
            "Detected " + nullCount + " records with null IDs due to buffer corruption");
    }

    /**
     * More aggressive test that repeatedly calls getAllAsync to increase
     * chances of buffer pool exhaustion and reuse.
     */
    @Test
    public void testGetAllBufferCorruptionWithConcurrentRequests() throws Exception {
        Table table = defaultTable();
        RecordView<Tuple> view = table.recordView();

        // Insert test data
        List<Tuple> insertData = new ArrayList<>();
        for (long i = 1; i <= 50; i++) {
            insertData.add(tuple(i, "Person_" + i));
        }
        view.upsertAll(null, insertData);

        // Prepare keys
        List<Tuple> keys = new ArrayList<>();
        for (long i = 1; i <= 50; i++) {
            keys.add(tupleKey(i));
        }

        // Fire multiple getAllAsync requests concurrently to exhaust buffer pool
        List<CompletableFuture<List<Tuple>>> futures = new ArrayList<>();

        for (int iteration = 0; iteration < 20; iteration++) {
            CompletableFuture<List<Tuple>> future = view.getAllAsync(null, keys);
            futures.add(future);

            // Don't wait - let them all execute concurrently
            Thread.sleep(10);
        }

        // Now wait for all and verify
        int totalCorruptions = 0;

        for (int i = 0; i < futures.size(); i++) {
            try {
                List<Tuple> results = futures.get(i).get(5, TimeUnit.SECONDS);

                for (Tuple result : results) {
                    if (result != null) {
                        Long id = result.longValue("id");
                        String name = result.stringValue("name");

                        if (id != null) {
                            String expectedName = "Person_" + id;
                            if (!expectedName.equals(name)) {
                                totalCorruptions++;
                                System.err.println("Request " + i + ": CORRUPTION at id=" + id
                                    + ", expected='" + expectedName + "', got='" + name + "'");
                            }
                        }
                    }
                }
            } catch (ExecutionException e) {
                System.err.println("Request " + i + " failed: " + e.getMessage());
                throw e;
            }
        }

        assertEquals(0, totalCorruptions,
            "Detected " + totalCorruptions + " corrupted records across all requests");
    }

    /**
     * Test that specifically targets the reduceWithKeepOrder path where
     * results from multiple partitions are merged while buffers might be freed.
     */
    @Test
    public void testGetAllBufferCorruptionDuringReduce() throws Exception {
        Table table = defaultTable();
        RecordView<Tuple> view = table.recordView();

        // Insert data with specific IDs that will hash to different partitions
        List<Tuple> insertData = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            insertData.add(tuple(i, "Data_" + i));
        }
        view.upsertAll(null, insertData);

        // Get keys in specific order
        List<Tuple> keys = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            keys.add(tupleKey(i));
        }

        CompletableFuture<List<Tuple>> future = view.getAllAsync(null, keys);

        // Wait a bit to let the requests complete but before accessing results
        Thread.sleep(150);

        // Generate more traffic to reuse buffers
        List<CompletableFuture<Void>> noise = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            noise.add(view.upsertAsync(null, tuple(1000L + i, "Noise_" + i)));
        }

        // Wait for noise to complete
        CompletableFuture.allOf(noise.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);

        // Force GC
        System.gc();
        Thread.sleep(100);

        // Now access the original results
        List<Tuple> results = future.get(1, TimeUnit.SECONDS);

        // Convert to list to check order preservation
        List<Tuple> resultList = new ArrayList<>(results);

        // Verify data integrity
        for (int i = 0; i < resultList.size(); i++) {
            Tuple t = resultList.get(i);
            assertNotNull(t, "Tuple at index " + i + " should not be null");

            Long id = t.longValue("id");
            String name = t.stringValue("name");

            assertNotNull(id, "ID should not be null at index " + i);
            assertEquals("Data_" + id, name,
                "Data corruption at index " + i + ": expected 'Data_" + id + "', got '" + name + "'");
        }
    }
}
