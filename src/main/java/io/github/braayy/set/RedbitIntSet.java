package io.github.braayy.set;

public class RedbitIntSet extends RedbitSet<Integer> {

    public RedbitIntSet(String name, String id) {
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
