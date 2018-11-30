package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.graph.NamedQuery;
import com.amazonaws.services.neptune.graph.ConcurrencyConfig;
import com.amazonaws.services.neptune.graph.NeptuneClient;
import com.amazonaws.services.neptune.util.Timer;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueryJob {
    private final Queue<NamedQuery> queries;
    private final NeptuneClient.QueryClient queryClient;
    private final ConcurrencyConfig concurrencyConfig;
    private final Directories directories;
    private final Format format;

    public QueryJob(Collection<NamedQuery> queries,
                    NeptuneClient.QueryClient queryClient,
                    ConcurrencyConfig concurrencyConfig,
                    Directories directories,
                    Format format){
        this.queries = new ConcurrentLinkedQueue<>(queries);
        this.queryClient = queryClient;
        this.concurrencyConfig = concurrencyConfig;
        this.directories = directories;
        this.format = format;
    }

    public void execute() throws Exception {
        try (Timer timer = new Timer()) {
            System.err.println("Writing " + format.description() + " files from queries");

            Status status = new Status();

            ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

            for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
                QueryTask queryTask = new QueryTask(queries, queryClient, directories, format, status, index);
                taskExecutor.execute(queryTask);
            }

            taskExecutor.shutdown();

            try {
                taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
