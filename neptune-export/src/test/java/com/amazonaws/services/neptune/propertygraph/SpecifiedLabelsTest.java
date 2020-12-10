/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.neptune.export.LabModeFeatures;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class SpecifiedLabelsTest {

    @Test
    public void shouldCreateLabelFilterForSimpleSingleNodeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("label1")),
                NodeLabelStrategy.nodeLabelsOnly);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.V(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.V().hasLabel(\"label1\")",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForComplexSingleNodeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("label1;label2")),
                NodeLabelStrategy.nodeLabelsOnly);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.V(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.V().hasLabel(\"label1\").hasLabel(\"label2\")",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterWithOrForMultipleSimpleNodeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Arrays.asList(new Label("label1"), new Label("label2")),
                NodeLabelStrategy.nodeLabelsOnly);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.V(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.V().or(__.hasLabel(\"label1\"),__.hasLabel(\"label2\"))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterWithOrForMultipleComplexNodeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Arrays.asList(new Label("label1;labelA"), new Label("label2;labelB")),
                NodeLabelStrategy.nodeLabelsOnly);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.V(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.V().or(__.hasLabel(\"label1\").hasLabel(\"labelA\"),__.hasLabel(\"label2\").hasLabel(\"labelB\"))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForSimpleEdgeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("edgeLabel1", "startLabel", "endLabel")),
                EdgeLabelStrategy.edgeLabelsOnly);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.E(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.E().hasLabel(\"edgeLabel1\")",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForComplexEdgeLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("edgeLabel1", "startLabel", "endLabel")),
                EdgeLabelStrategy.edgeAndVertexLabels);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.E(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.E().hasLabel(\"edgeLabel1\").where(__.and(__.outV().hasLabel(\"startLabel\"),__.inV().hasLabel(\"endLabel\")))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForComplexEdgeLabelWithComplexVertexLabels(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("edgeLabel1", "startLabel1;startLabel2", "endLabel1;endLabel2")),
                EdgeLabelStrategy.edgeAndVertexLabels);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.E(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.E().hasLabel(\"edgeLabel1\").where(__.and(__.outV().hasLabel(\"startLabel1\").hasLabel(\"startLabel2\"),__.inV().hasLabel(\"endLabel1\").hasLabel(\"endLabel2\")))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForComplexEdgeLabelWithOnlyStartVertexLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("edgeLabel1", "startLabel", "")),
                EdgeLabelStrategy.edgeAndVertexLabels);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.E(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.E().hasLabel(\"edgeLabel1\").where(__.outV().hasLabel(\"startLabel\"))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

    @Test
    public void shouldCreateLabelFilterForComplexEdgeLabelWithOnlyEndVertexLabel(){

        SpecifiedLabels specifiedLabels = new SpecifiedLabels(
                Collections.singletonList(new Label("edgeLabel1", "", "endLabel")),
                EdgeLabelStrategy.edgeAndVertexLabels);

        AnonymousTraversalSource<GraphTraversalSource> traversalSource = AnonymousTraversalSource.traversal();
        GraphTraversalSource g = traversalSource.withGraph(EmptyGraph.instance());

        GraphTraversal<? extends Element, ?> traversal =
                specifiedLabels.apply(g.E(), new LabModeFeatures(Collections.emptyList()));

        assertEquals("__.E().hasLabel(\"edgeLabel1\").where(__.inV().hasLabel(\"endLabel\"))",
                GremlinQueryDebugger.queryAsString(traversal));
    }

}