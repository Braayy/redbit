package io.github.braayy.list;

public class RedbitStringList extends RedbitList<String> {

    public RedbitStringList(String name, String id) {
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
