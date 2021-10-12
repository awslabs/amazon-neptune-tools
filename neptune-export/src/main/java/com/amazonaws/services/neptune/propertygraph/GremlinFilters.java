/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;

public class GremlinFilters {

    public static final GremlinFilters EMPTY = new GremlinFilters(null, null, null);

    private final String gremlinFilter;
    private final String gremlinNodeFilter;
    private final String gremlinEdgeFilter;

    private static final List<String> INVALID_OPERATORS = Arrays.asList("addV", "addE", "write", "drop", "sideEffect", "property");

    public GremlinFilters(String gremlinFilter, String gremlinNodeFilter, String gremlinEdgeFilter) {
        this.gremlinFilter = gremlinFilter;
        this.gremlinNodeFilter = gremlinNodeFilter;
        this.gremlinEdgeFilter = gremlinEdgeFilter;
    }

    public GraphTraversal<? extends Element, ?> applyToNodes(GraphTraversal<? extends Element, ?> t) {
        if (StringUtils.isNotEmpty(gremlinNodeFilter)) {
            return apply(t, gremlinNodeFilter);
        } else if (StringUtils.isNotEmpty(gremlinFilter)) {
            return apply(t, gremlinFilter);
        } else {
            return t;
        }
    }

    public GraphTraversal<? extends Element, ?> applyToEdges(GraphTraversal<? extends Element, ?> t) {
        if (StringUtils.isNotEmpty(gremlinEdgeFilter)) {
            return apply(t, gremlinEdgeFilter);
        } else if (StringUtils.isNotEmpty(gremlinFilter)) {
            return apply(t, gremlinFilter);
        } else {
            return t;
        }
    }

    private GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> t, String gremlin) {
        CachedGremlinScriptEngineManager scriptEngineManager = new CachedGremlinScriptEngineManager();
        GremlinScriptEngine engine = scriptEngineManager.getEngineByName("gremlin-groovy");
        Bindings engineBindings = engine.createBindings();
        engineBindings.put("datetime", new DatetimeConverter());

        Traversal.Admin<?, ?> whereTraversal = null;
        try {
            whereTraversal = (Traversal.Admin) engine.eval(gremlin, engineBindings);
        } catch (ScriptException e) {
            throw new IllegalStateException(String.format("Invalid Gremlin filter: %s. %s", gremlin, e.getMessage()), e);
        }

        for (Step<?, ?> step : whereTraversal.getSteps()) {

            for (Bytecode.Instruction instruction : step.getTraversal().getBytecode().getInstructions()) {
                String operator = instruction.getOperator();
                validateOperator(operator);
                t.asAdmin().getBytecode().addStep(operator, instruction.getArguments());
            }
            t.asAdmin().addStep(step);
        }

        return t;
    }

    private void validateOperator(String operator) {
        if (INVALID_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException(String.format("Invalid operator: '%s'. Gremlin filter cannot contain side effect or mutating step.", operator));
        }
    }

    private static class DatetimeConverter {
        private static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTimeParser().withZoneUTC();

        public Object call(String args) {
            return dateTimeFormatter.parseDateTime(args).toDate();
        }
    }
}
