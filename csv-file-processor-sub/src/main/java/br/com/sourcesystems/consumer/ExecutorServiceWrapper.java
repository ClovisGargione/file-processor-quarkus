package br.com.sourcesystems.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceWrapper implements AutoCloseable {

    private final ExecutorService executor;

    public ExecutorServiceWrapper(ExecutorService executor) {
        this.executor = executor;
    }

    public void submit(Runnable task) {
        executor.submit(task);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}