package com.amazonaws.services.neptune.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class NamedQueriesCollection {

    public static NamedQueriesCollection fromJson(ArrayNode json) {
        List<NamedQueries> collection = new ArrayList<>();

        for (JsonNode jsonNode : json) {
            collection.add(NamedQueries.fromJson(jsonNode));
        }

        return new NamedQueriesCollection(collection);
    }


    private final Collection<NamedQueries> namedQueries;

    public NamedQueriesCollection(Collection<NamedQueries> namedQueries) {
        this.namedQueries = namedQueries;
    }

    public Collection<NamedQuery> flatten() {
        List<NamedQuery> queries = new ArrayList<>();
        namedQueries.forEach(q -> q.addTo(queries));
        return queries;
    }

    public Collection<String> names(){
        return namedQueries.stream().map(NamedQueries::name).collect(Collectors.toList());
    }

    public ArrayNode toJson() {
        ArrayNode json = JsonNodeFactory.instance.arrayNode();

        for (NamedQueries queries : namedQueries) {

            ObjectNode queriesNode = JsonNodeFactory.instance.objectNode();
            ArrayNode arrayNode = queries.toJson();

            queriesNode.put("name", queries.name());
            queriesNode.set("queries", arrayNode);

            json.add(queriesNode);
        }

        return json;
    }
}
