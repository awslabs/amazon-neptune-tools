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

package com.amazonaws.services.neptune.export;

import com.amazonaws.services.neptune.NeptuneExportCli;
import com.amazonaws.services.neptune.NeptuneExportCommand;
import com.amazonaws.services.neptune.NeptuneExportEventHandlerHost;
import org.apache.commons.lang.StringUtils;

public class NeptuneExportRunner {

    private final String[] args;
    private final NeptuneExportEventHandler eventHandler;
    private final boolean isCliInvocation;

    public NeptuneExportRunner(String[] args) {
        this(args, NeptuneExportEventHandler.NULL_EVENT_HANDLER, true);
    }

    public NeptuneExportRunner(String[] args, NeptuneExportEventHandler eventHandler, boolean isCliInvocation) {
        this.args = args;
        this.eventHandler = eventHandler;
        this.isCliInvocation = isCliInvocation;
    }

    public void run() {

        Args argsCollection = new Args(this.args);
        if (argsCollection.contains("--log-level")){
            String logLevel = argsCollection.getFirstOptionValue("--log-level");
            if (StringUtils.isNotEmpty(logLevel)){
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", logLevel);
            }
        }

        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(NeptuneExportCli.class);

        try {
            Runnable cmd = cli.parse(this.args);

            if (NeptuneExportEventHandlerHost.class.isAssignableFrom(cmd.getClass())) {
                NeptuneExportEventHandlerHost eventHandlerHost = (NeptuneExportEventHandlerHost) cmd;
                eventHandlerHost.setEventHandler(eventHandler);
            }

            if (NeptuneExportCommand.class.isAssignableFrom(cmd.getClass())){
                ((NeptuneExportCommand) cmd).setIsCliInvocation(isCliInvocation);
            }

            cmd.run();
        } catch (Exception e) {

            System.err.println(e.getMessage());
            System.err.println();

            System.exit(-1);
        }
    }
}
