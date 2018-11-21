package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.DataType;
import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class PropertyCsvWriter {

    private final Map<String, PropertyTypeInfo> metadata;
    private final boolean printLeadingComma;

    public PropertyCsvWriter(Map<String, PropertyTypeInfo> metadata, boolean printLeadingComma) {
        this.metadata = metadata;
        this.printLeadingComma = printLeadingComma;
    }

    void handle(Map<?, ?> properties, PrintWriter writer) {

        boolean printComma = false;

        for (Map.Entry<String, PropertyTypeInfo> entry : metadata.entrySet()) {

            String property = entry.getKey();
            DataType dataType = entry.getValue().dataType();

            if (properties.containsKey(property)) {
                printComma = printComma(printComma, writer);

                Object value = properties.get(property);
                String formattedValue = isList(value) ? formatList(value, dataType) : dataType.format(value);
                writer.print(formattedValue);

            } else {
                printComma = printComma(printComma, writer);
            }
        }
    }

    private boolean printComma(boolean printComma, PrintWriter writer){
        if (printLeadingComma || printComma) {
            writer.print(",");
        }
        return true;
    }

    private String formatList(Object value, DataType dataType) {
        List<?> values = (List<?>) value;
        return dataType.formatList(values);
    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }
}
