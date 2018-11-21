package com.amazonaws.services.neptune.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NamedQueries {

    public static NamedQueries fromJson(JsonNode json) {
        String name = json.path("name").textValue();
        ArrayNode queries = (ArrayNode) json.path("queries");

        List<String> collection = new ArrayList<>();

        for (JsonNode query : queries) {
            collection.add(query.textValue());
        }

        return new NamedQueries(name, collection);
    }

    private final String name;
    private final Collection<String> queries;

    public NamedQueries(String name, Collection<String> queries) {
        this.name = name;
        this.queries = queries;
    }

    public String name() {
        return name;
    }

    public Collection<String> queries() {
        return queries;
    }

    public void addTo(Collection<NamedQuery> namedQueries) {
        for (String query : queries) {
            namedQueries.add(new NamedQuery(name, query));
        }
    }

    public ArrayNode toJson() {
        ArrayNode json = JsonNodeFactory.instance.arrayNode();

        for (String query : queries) {
            json.add(query);
        }

        return json;
    }
}
