package com.amazonaws.services.neptune.graph;

public class NamedQuery {
    private final String name;
    private final String query;

    public NamedQuery(String name, String query) {
        this.name = name;
        this.query = query;
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }
}
