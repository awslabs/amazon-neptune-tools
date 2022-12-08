/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.io.StatusOutputFormat;
import com.amazonaws.services.neptune.propertygraph.NamedQuery;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.util.Activity;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryJob {
    private final Queue<NamedQuery> queries;
    private final NeptuneGremlinClient.QueryClient queryClient;
    private final ConcurrencyConfig concurrencyConfig;
    private final PropertyGraphTargetConfig targetConfig;
    private final boolean twoPassAnalysis;
    private final Long timeoutMillis;

    public QueryJob(Collection<NamedQuery> queries,
                    NeptuneGremlinClient.QueryClient queryClient,
                    ConcurrencyConfig concurrencyConfig,
                    PropertyGraphTargetConfig targetConfig,
                    boolean twoPassAnalysis,
                    Long timeoutMillis){
        this.queries = new ConcurrentLinkedQueue<>(queries);
        this.queryClient = queryClient;
        this.concurrencyConfig = concurrencyConfig;
        this.targetConfig = targetConfig;
        this.twoPassAnalysis = twoPassAnalysis;
        this.timeoutMillis = timeoutMillis;
    }

    public void execute() throws Exception {
        Timer.timedActivity("exporting results from queries", (CheckedActivity.Runnable) this::export);
    }

    private void export() throws ExecutionException, InterruptedException {

        System.err.println("Writing query results to " + targetConfig.output().name() + " as " + targetConfig.format().description());

        Status status = new Status(StatusOutputFormat.Description, "query results");

        ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

        Collection<Future<Object>> futures = new ArrayList<>();

        AtomicInteger fileIndex = new AtomicInteger();

        for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
            QueryTask queryTask = new QueryTask(
                    queries,
                    queryClient,
                    targetConfig,
                    twoPassAnalysis,
                    timeoutMillis,
                    status,
                    fileIndex);
            futures.add(taskExecutor.submit(queryTask));
        }

        taskExecutor.shutdown();

        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        for (Future<Object> future : futures) {
            if (future.isCancelled()) {
                throw new IllegalStateException("Unable to complete job because at least one task was cancelled");
            }
            if (!future.isDone()) {
                throw new IllegalStateException("Unable to complete job because at least one task has not completed");
            }
            future.get();
        }
    }
}
