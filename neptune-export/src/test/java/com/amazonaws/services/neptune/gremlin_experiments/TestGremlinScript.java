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

package com.amazonaws.services.neptune.gremlin_experiments;

import com.amazonaws.services.neptune.propertygraph.GremlinQueryDebugger;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestGremlinScript {

    @Test
    public void testGremlinScript() throws ScriptException {

        CachedGremlinScriptEngineManager scriptEngineManager = new CachedGremlinScriptEngineManager();
        GremlinScriptEngine engine = scriptEngineManager.getEngineByName("gremlin-groovy");

        List<GremlinScriptEngineFactory> factories = scriptEngineManager.getEngineFactories();

        for (GremlinScriptEngineFactory factory : factories) {
            System.out.println(factory.getEngineName());
        }

        Graph graph = EmptyGraph.instance();

        GraphTraversalSource g = graph.traversal();
        GraphTraversal<Vertex, Vertex> t1 = g.V();

        ScriptTraversal scriptTraversal = new ScriptTraversal(g, "gremlin-groovy", "V().limit(10)");
        scriptTraversal.applyStrategies();


        Bindings engineBindings = engine.createBindings();

        Traversal.Admin<?, ?> traversal = (Traversal.Admin) engine.eval("has(\"name\", \"ian\")", engineBindings);
        List<Step> steps = traversal.getSteps();



        for (Step step : steps) {
            System.out.println(step);
            t1 = t1.asAdmin().addStep(step);
        }

        List<Step> steps1 = t1.asAdmin().getSteps();
        System.out.println("---");
        for (Step step : steps1) {
            System.out.println(step);
        }

        t1.limit(1);


        System.out.println("---");

        t1.forEachRemaining(s -> System.out.println(s));


        System.out.println(GremlinQueryDebugger.queryAsString(t1.asAdmin()));
    }
}
