package io.github.braayy.set;

public class RedbitStringSet extends RedbitSet<String> {

    public RedbitStringSet(String name, String id) {
        super(name, id);
    }

    @Override
    public String fromString(String input) {
        return input;
    }

    @Override
    public String toString(String input) {
        return input;
    }

}
