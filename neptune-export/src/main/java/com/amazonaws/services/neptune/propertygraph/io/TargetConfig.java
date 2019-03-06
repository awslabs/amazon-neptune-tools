package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.Directories;

public class TargetConfig {

    private final Directories directories;
    private final Format format;
    private final Output output;

    public TargetConfig(Directories directories, Format format, Output output) {
        this.directories = directories;
        this.format = format;
        this.output = output;
    }

    public Directories directories() {
        return directories;
    }

    public Format format() {
        return format;
    }

    public Output output() {
        return output;
    }
}
