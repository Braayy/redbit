package io.github.braayy.fetch;

import io.github.braayy.struct.RedbitStruct;

public abstract class RedbitFetch implements AutoCloseable {

    protected final RedbitStruct struct;

    public RedbitFetch(RedbitStruct struct) {
        this.struct = struct;
    }

    public abstract Result next();

    public enum Result {
        ERROR, FOUND, NOT_FOUND, COMPLETE
    }
}
