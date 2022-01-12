package io.github.braayy.synchronization;

import io.github.braayy.Redbit;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedbitSynchronizer extends Thread {

    private final List<RedbitSynchronizationEntry> modifiedEntries = new ArrayList<>();
    private final Lock modifiedLock = new ReentrantLock();
    private final Lock remainingLock = new ReentrantLock();
    private final Queue<RedbitSynchronizationEntry> remainingEntries = new ArrayDeque<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(Redbit.getConfig().getParallelTasks() + 1);

    private final AtomicBoolean synchronizing = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public RedbitSynchronizer() {
        super("Redbit Synchronizer Thread");
    }

    @Override
    public void run() {
        while (true) {
            if (!shutdown.get()) continue;

            remainingLock.lock();
            try {
                if (remainingEntries.size() == 0)
                    return;
            } finally {
                remainingLock.unlock();
            }
        }
    }

    void synchronize() {
        modifiedLock.lock();
        remainingLock.lock();
        try {
            if (modifiedEntries.size() == 0) return;

            remainingEntries.addAll(modifiedEntries);
            modifiedEntries.clear();

            if (synchronizing.compareAndSet(false, true)) {
                nextBatch();
            }
        } finally {
            modifiedLock.unlock();
            remainingLock.unlock();
        }
    }

    public void shutdown() {
        shutdown.set(true);
        CompletableFuture.runAsync(this::synchronize, executorService);
        executorService.shutdown();
    }

    public boolean isShuttingDown() {
        return shutdown.get();
    }

    public void addModifiedKey(RedbitStructInfo structInfo, String idValue, Operation operation) {
        modifiedLock.lock();
        try {
            this.modifiedEntries.removeIf(entry -> entry.getIdValue().equals(idValue) && entry.getOperation() == operation);
            this.modifiedEntries.add(new RedbitSynchronizationEntry(structInfo, idValue, operation));
        } finally {
            modifiedLock.unlock();
        }
    }

    @SuppressWarnings("rawtypes")
    private void nextBatch() {
        remainingLock.lock();

        try {
            int parallelTasks = Math.min(Redbit.getConfig().getParallelTasks(), remainingEntries.size());

            if (parallelTasks <= 0) {
                synchronizing.set(false);
                return;
            }

            CompletableFuture[] tasks = new CompletableFuture[parallelTasks];
            for (int i = 0; i < parallelTasks; i++) {
                RedbitSynchronizationEntry currentEntry = remainingEntries.poll();

                tasks[i] = CompletableFuture
                        .runAsync(new RedbitSynchronizationTask(currentEntry), executorService);
            }

            CompletableFuture
                    .allOf(tasks)
                    .thenRun(this::nextBatch);
        } finally {
            remainingLock.unlock();
        }
    }

}
