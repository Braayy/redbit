package io.github.braayy.list;

public class RedbitIntList extends RedbitList<Integer> {

    public RedbitIntList(String key) {
        super(key);
    }

    @Override
    public Integer transform(String input) {
        return Integer.parseInt(input);
    }
}
