package io.github.braayy.synchronization;

import io.github.braayy.Redbit;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedbitSynchronizer {

    private final List<RedbitSynchronizationEntry> modifiedEntries = new ArrayList<>();

    private final Queue<RedbitSynchronizationEntry> remainingEntries = new ArrayDeque<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(Redbit.getConfig().getParallelTasks());

    private boolean completed = true;

    public void synchronize() {
        if (modifiedEntries.size() == 0 || !completed) return;

        remainingEntries.clear();
        remainingEntries.addAll(modifiedEntries);
        completed = false;

        modifiedEntries.clear();

        nextKeys();
    }

    public void addModifiedKey(RedbitStructInfo structInfo, String idValue, Operation operation) {
        this.modifiedEntries.removeIf(entry -> entry.getIdValue().equals(idValue));
        this.modifiedEntries.add(new RedbitSynchronizationEntry(structInfo, idValue, operation));
    }

    @SuppressWarnings("rawtypes")
    private void nextKeys() {
        int parallelTasks = Math.min(Redbit.getConfig().getParallelTasks(), remainingEntries.size());

        if (parallelTasks <= 0) {
            completed = true;
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
                .thenRun(this::nextKeys);
    }

}
