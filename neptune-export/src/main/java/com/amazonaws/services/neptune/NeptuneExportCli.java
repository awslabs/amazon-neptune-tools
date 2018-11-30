package com.amazonaws.services.neptune;

import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.help.Help;

@Cli(name = "neptune-export.sh",
        description = "Export Neptune to CSV or JSON",
        defaultCommand = Help.class,
        commands = {Export.class, CreateConfig.class, ExportFromConfig.class, ExportFromQueries.class, Help.class})
public class NeptuneExportCli {

    public static void main(String[] args) {
        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(NeptuneExportCli.class);

        try {

            Runnable cmd = cli.parse(args);
            cmd.run();

        } catch (Exception e) {

            System.err.println(e.getMessage());
            System.err.println();

            Runnable cmd = cli.parse("help", args[0]);
            cmd.run();

            System.exit(-1);
        }
    }
}
