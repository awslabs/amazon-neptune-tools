package com.amazonaws.services.neptune.propertygraph;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;

public class GremlinQueryDebugger {

    public static String queryAsString(Object o){
        return String.valueOf(new GroovyTranslator.DefaultTypeTranslator().apply("g", o));
    }
}
