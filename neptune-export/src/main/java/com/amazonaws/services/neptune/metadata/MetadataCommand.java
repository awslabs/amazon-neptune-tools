package com.amazonaws.services.neptune.metadata;

public interface MetadataCommand {
    PropertiesMetadataCollection execute() throws Exception;
}
