package io.github.braayy.list;

public class RedbitIntList extends RedbitList<Integer> {

    public RedbitIntList(String name, String id) {
        super(name, id);
    }

    @Override
    public Integer fromString(String input) {
        return Integer.parseInt(input);
    }

    @Override
    public String toString(Integer input) {
        return input.toString();
    }

}
