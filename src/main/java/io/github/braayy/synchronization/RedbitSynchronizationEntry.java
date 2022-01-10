package io.github.braayy.synchronization;

import io.github.braayy.struct.RedbitStructInfo;

public class RedbitSynchronizationEntry {

    private final RedbitStructInfo structInfo;
    private final String idValue;
    private final Operation operation;

    public RedbitSynchronizationEntry(RedbitStructInfo structInfo, String idValue, Operation operation) {
        this.structInfo = structInfo;
        this.idValue = idValue;
        this.operation = operation;
    }

    public RedbitStructInfo getStructInfo() {
        return structInfo;
    }

    public String getIdValue() {
        return idValue;
    }

    public Operation getOperation() {
        return operation;
    }

    public enum Operation {
        UPSERT, DELETE, DELETE_ALL
    }

}
