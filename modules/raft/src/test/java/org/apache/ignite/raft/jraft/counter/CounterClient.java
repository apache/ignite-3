/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.counter;

import org.apache.ignite.raft.jraft.RouteTable;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.impl.cli.CliClientServiceImpl;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.counter.rpc.IncrementAndGetRequest;

public class CounterClient {

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Useage : java org.apache.ignite.raft.jraft.example.counter.CounterClient {groupId} {conf}");
            System.out
                .println("Example: java org.apache.ignite.raft.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String groupId = args[0];
        final String confStr = args[1];

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 1;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            incrementAndGet(cliClientService, leader, i, latch);
        }
        latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        System.exit(0);
    }

    private static void incrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader,
                                        final long delta, CountDownLatch latch) throws RemotingException,
        InterruptedException {
        final IncrementAndGetRequest request = new IncrementAndGetRequest();
        request.setDelta(delta);
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("incrementAndGet result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

}
