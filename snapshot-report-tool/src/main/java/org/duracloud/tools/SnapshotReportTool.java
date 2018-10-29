package org.duracloud.tools;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.duracloud.client.ContentStore;
import org.duracloud.client.ContentStoreManager;
import org.duracloud.client.ContentStoreManagerImpl;
import org.duracloud.client.task.SnapshotTaskClient;
import org.duracloud.client.task.SnapshotTaskClientImpl;
import org.duracloud.common.model.Credential;
import org.duracloud.error.ContentStoreException;
import org.duracloud.snapshot.dto.SnapshotSummary;
import org.duracloud.snapshot.dto.task.GetSnapshotListTaskResult;
import org.duracloud.snapshot.dto.task.GetSnapshotTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * SnapshotReportTool - Provides simple report on an account's snapshots
 *
 * @author: Bill Branan
 * Date: Oct 29, 2018
 */
public class SnapshotReportTool {

    private final Logger log = LoggerFactory.getLogger(SnapshotReportTool.class);

    private String duracloudHost;
    private String duracloudUsername;
    private String duracloudPassword;

    private static Options cmdOptions;

    public SnapshotReportTool(String duracloudHost,
                              String duracloudUsername,
                              String duracloudPassword) {
        this.duracloudHost = duracloudHost;
        this.duracloudUsername = duracloudUsername;
        this.duracloudPassword = duracloudPassword;
    }

    /**
     * Kicks off the execution of the tool
     *
     * @throws IOException
     */
    public void run() throws ContentStoreException {
        ContentStoreManager storeManager = new ContentStoreManagerImpl(duracloudHost, "443");
        Credential credential = new Credential(duracloudUsername, duracloudPassword);
        storeManager.login(credential);

        ContentStore store = storeManager.getPrimaryContentStore();
        SnapshotTaskClient taskClient = new SnapshotTaskClientImpl(store);

        List<Long> snapshotByteTotals = new LinkedList<>();
        long totalSnapshotBytes = 0;

        GetSnapshotListTaskResult snapshotListTaskResult = taskClient.getSnapshots();
        for (SnapshotSummary snapshotSummary : snapshotListTaskResult.getSnapshots()) {
            String snapshotId = snapshotSummary.getSnapshotId();
            GetSnapshotTaskResult snapshotTaskResult = taskClient.getSnapshot(snapshotId);
            long snapshotByteSize = snapshotTaskResult.getTotalSizeInBytes();
            snapshotByteTotals.add(snapshotByteSize);
            totalSnapshotBytes += snapshotByteSize;
        }

        int allSnapshotsCount = snapshotListTaskResult.getSnapshots().size();
        double allSnapshotsBytes = (double) totalSnapshotBytes / 1000000000;

        System.out.println("Snapshots for: " + duracloudHost);
        System.out.println("  Number of snapshots: " + allSnapshotsCount);
        System.out.println("  Total size of all snapshots: " +
                           String.format("%.02f", allSnapshotsBytes) + " GB");
        System.out.println("  Individual snapshot size (in GB):");
        for (Long snapshotByteTotal : snapshotByteTotals) {
            double snapshotBytes =  (double) snapshotByteTotal / 1000000000;
            System.out.println(String.format("%.02f", snapshotBytes));
        }
    }

    /**
     * Manages the command line execution, including all command line parameters
     *
     * @param args - command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        cmdOptions = new Options();

        Option duracloudHostOption =
            new Option("h", "duracloud-host", true,
                       "the host at which the duracloud app can be found");
        duracloudHostOption.setRequired(true);
        cmdOptions.addOption(duracloudHostOption);

        Option usernameOption =
            new Option("u", "duracloud-username", true,
                       "the username necessary to read from duracloud");
        usernameOption.setRequired(true);
        cmdOptions.addOption(usernameOption);

        Option passwordOption =
            new Option("p", "duracloud-password", true,
                       "the password necessary to read from duracloud");
        passwordOption.setRequired(true);
        cmdOptions.addOption(passwordOption);

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            usage();
        }

        String duracloudHost = cmd.getOptionValue("h");
        String duracloudUsername = cmd.getOptionValue("u");
        String duracloudPassword = cmd.getOptionValue("p");

        SnapshotReportTool tool = new SnapshotReportTool(duracloudHost,
                                                         duracloudUsername,
                                                         duracloudPassword);
        tool.run();
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running the Snapshot Report Tool", cmdOptions);
        System.exit(1);
    }

}