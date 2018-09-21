package org.duracloud.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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
import org.duracloud.common.model.Credential;
import org.duracloud.common.util.DateUtil;
import org.duracloud.error.ContentStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Provider Manifest Tool - Generates a manifest of content directly from a DuraCloud storage provider
 *
 * @author: Bill Branan
 */
public class ProviderManifestTool {

    private static Logger log = LoggerFactory.getLogger(ProviderManifestTool.class);
    private static final String DEFAULT_PORT = "443";
    private static final String DEFAULT_CONTEXT = "durastore";

    private String host;
    private String port;
    private String username;
    private String password;
    private String storeId;
    private String spaceId;
    private String outputFileName;

    private static Options cmdOptions;

    public ProviderManifestTool(String host,
                                String port,
                                String username,
                                String password,
                                String storeId,
                                String spaceId) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.storeId = storeId;
        this.spaceId = spaceId;

        this.outputFileName = spaceId + "-provider-manifest-" + DateUtil.nowPlain() + ".tsv";
    }

    /**
     * Kicks off the execution of the tool.
     *
     * @throws ContentStoreException
     * @throws IOException
     */
    public void run() throws ContentStoreException, IOException {
        log.info("\n-----------------------------------------" +
                 "\nRunning Provider Manifest Tool with config:" +
                 "\nhost={}\nport={}\nspace name={}" +
                 "\n-----------------------------------------", host, port, spaceId);

        log.info("Setting up tool...");
        final ContentStoreManager storeManager =
            new ContentStoreManagerImpl(host, port, DEFAULT_CONTEXT);
        final Credential credential = new Credential(username, password);
        storeManager.login(credential);

        final ContentStore store;
        if (storeId == null || storeId.equals("")) {
            store = storeManager.getPrimaryContentStore();
            this.storeId = store.getStoreId();
        } else {
            store = storeManager.getContentStore(storeId);
        }

        final File file = new File(this.outputFileName);
        log.info("Writing to output file: " + file.getAbsolutePath());

        try (BufferedWriter is = new BufferedWriter(new FileWriter(file))) {
            // Write TSV header
            is.write("space-id\tcontent-id\tMD5");
            is.newLine();

            Iterator<String> contentItems = store.getSpaceContents(spaceId);
            while (contentItems.hasNext()) {
                String contentId = contentItems.next();
                Map<String, String> contentProps = store.getContentProperties(spaceId, contentId);
                String checksum = contentProps.get(ContentStore.CONTENT_CHECKSUM);

                // Write details about content itme to output files
                is.write(spaceId + "\t" + contentId + "\t" + checksum);
                is.newLine();
                is.flush();
            }
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

        Option spaceFileOption =
            new Option("s", "space", true,
                       "the ID of the space to be listed");
        spaceFileOption.setRequired(true);
        cmdOptions.addOption(spaceFileOption);

        Option hostOption =
            new Option("h", "host", true,
                       "the host address of the DuraCloud DuraStore application");
        hostOption.setRequired(true);
        cmdOptions.addOption(hostOption);

        Option portOption =
            new Option("t", "port", true,
                       "the port of the DuraCloud DuraStore application " +
                       "(optional, default value is " + DEFAULT_PORT + ")");
        portOption.setRequired(false);
        cmdOptions.addOption(portOption);

        Option usernameOption =
            new Option("u", "username", true,
                       "the username necessary to perform writes to DuraStore");
        usernameOption.setRequired(true);
        cmdOptions.addOption(usernameOption);

        Option passwordOption =
            new Option("p", "password", true,
                       "the password necessary to perform writes to DuraStore");
        passwordOption.setRequired(true);
        cmdOptions.addOption(passwordOption);

        Option storeIdOption =
            new Option("i", "store-id", true,
                       "the ID of the store (optional)");
        storeIdOption.setRequired(false);
        cmdOptions.addOption(storeIdOption);

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            log.info(e.getMessage());
            usage();
        }

        String host = cmd.getOptionValue("h");
        String username = cmd.getOptionValue("u");
        String password = cmd.getOptionValue("p");
        String storeId = cmd.getOptionValue("i");
        String spaceId = cmd.getOptionValue("s");

        String port = cmd.getOptionValue("t");
        if (port == null || port.equals("")) {
            port = DEFAULT_PORT;
        }

        ProviderManifestTool tool =
            new ProviderManifestTool(host, port, username, password, storeId, spaceId);
        tool.run();
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running the provider-manifest-tool", cmdOptions);
        System.exit(1);
    }

}