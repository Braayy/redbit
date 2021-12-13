package io.github.braayy.list;

public class RedbitStringList extends RedbitList<String> {

    public RedbitStringList(String key) {
        super(key);
    }

    @Override
    public String transform(String input) {
        return input;
    }
}
