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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.cli.LabModeModule;
import com.amazonaws.services.neptune.export.NeptuneExportEventHandler;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;

import javax.inject.Inject;
import java.nio.file.Path;
import java.util.Collection;

public abstract class NeptuneExportBaseCommand implements NeptuneExportEventHandler {

    @Option(name = {"--log-level"}, description = "Log level (optional, default 'error')", title = "log level")
    @Once
    @AllowedValues(allowedValues = {"trace", "debug", "info", "warn", "error"})
    protected String logLevel = "error";

    @Inject
    private LabModeModule labModeModule = new LabModeModule();

    private NeptuneExportEventHandler eventHandler = NeptuneExportEventHandler.NULL_EVENT_HANDLER;

    public void applyLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", logLevel);
    }

    public void setEventHandler(NeptuneExportEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void onExportComplete(Path outputPath, ExportStats stats){
        eventHandler.onExportComplete(outputPath, stats);
    }

    void handleException(Throwable e){
        if (e.getCause() != null && RemoteConnectionException.class.isAssignableFrom(e.getCause().getClass())){
            System.err.println("An error occurred while connecting to Neptune. " +
                    "Ensure you have specified the --use-ssl flag if the database requires SSL in transit. " +
                    "Ensure you have specified the --use-iam-auth flag if the database uses IAM database authentication.");
            e.printStackTrace();
        } else {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }

    Collection<String> labModeFeatures(){
        return labModeModule.labFeatures();
    }
}
