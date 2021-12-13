package io.github.braayy.synchronization;

import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedbitSynchronizer {

    private final List<RedbitSynchronizationEntry> modifiedEntries = new ArrayList<>();

    private final Queue<RedbitSynchronizationEntry> remainingKeys = new ArrayDeque<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    private boolean completed = true;

    public void synchronize() {
        if (modifiedEntries.size() == 0 || !completed) return;

        remainingKeys.clear();
        remainingKeys.addAll(modifiedEntries);
        modifiedEntries.clear();

        nextKey();
    }

    public void addModifiedKey(RedbitStructInfo structInfo, String idValue, Operation operation) {
        this.modifiedEntries.removeIf(entry -> entry.getIdValue().equals(idValue));
        this.modifiedEntries.add(new RedbitSynchronizationEntry(structInfo, idValue, operation));
    }

    private void nextKey() {
        RedbitSynchronizationEntry currentEntry = remainingKeys.poll();

        if (currentEntry == null) {
            completed = true;
            return;
        }

        CompletableFuture
                .runAsync(new RedbitSynchronizationTask(currentEntry), executorService)
                .thenRun(this::nextKey);
    }

}
