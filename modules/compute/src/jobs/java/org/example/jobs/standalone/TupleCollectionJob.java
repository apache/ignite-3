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

package org.example.jobs.standalone;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.table.Tuple;

/**
 * Returns the provided tuple collection with added job result tuple.
 */
public class TupleCollectionJob implements ComputeJob<Collection<Tuple>, Collection<Tuple>> {
    @Override
    public CompletableFuture<Collection<Tuple>> executeAsync(JobExecutionContext jobExecutionContext, Collection<Tuple> arg) {
        List<Tuple> res = new ArrayList<>(arg);
        res.add(Tuple.create().set("job_result", "done"));

        return completedFuture(res);
    }
}
