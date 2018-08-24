package org.duracloud.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.duracloud.common.util.IOUtil;
import org.duracloud.error.ContentStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copy Content Tool - Copies content from one space to another.
 *
 * @author: Daniel Bernstein
 */
public class CopyContentTool {

    private static Logger LOGGER = LoggerFactory.getLogger( CopyContentTool.class );
    private static final String DEFAULT_PORT = "443";
    private static final String DEFAULT_CONTEXT = "durastore";

    private String host;
    private String port;
    private String username;
    private String password;
    private String storeId;
    private String spaceListFilePath;
    private String inputSpaceRegex = "^(.*)-(open|campus|closed)$";
    private String destinationSpaceFormat = "${2}";
    private String destinationContentFormat = "${1}/${contentId}";
    private boolean dryRun;

    private static Options cmdOptions;

    public CopyContentTool(String host,
                           String port,
                           String username,
                           String password,
                           String storeId,
                           String spacesListFilePath,
                           boolean dryRun) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.storeId = storeId;
        this.dryRun = dryRun;
        this.spaceListFilePath = spacesListFilePath;
    }

    /**
     * Kicks off the execution of the tool.
     *
     * @throws ContentStoreException
     * @throws IOException
     */
    public void run() throws ContentStoreException, IOException {
        LOGGER.debug( "-----------------------------------------" +
                      "\nRunning Copy Content Tool with config:" +
                      "\nhost={}" + host +
                      "\nport={}" + port +
                      "\nspaces list file path={}" + spaceListFilePath +
                      (dryRun ? "\nThis execution is a DRY RUN - no changes will be made!" : "") +
                      "\n-----------------------------------------", host, port, spaceListFilePath );

        LOGGER.info( "Setting up tool..." );
        final ContentStoreManager storeManager =
            new ContentStoreManagerImpl( host, port, DEFAULT_CONTEXT );
        final Credential credential = new Credential( username, password );
        storeManager.login( credential );

        final ContentStore store;
        if (storeId == null || storeId.equals( "" )) {
            store = storeManager.getPrimaryContentStore();
            this.storeId = store.getStoreId();
        } else {
            store = storeManager.getContentStore( storeId );
        }

        final List<String> spaces;

        if (this.spaceListFilePath == null) {
            LOGGER.debug( "spaceListFilePath is not set; all spaces associated with the account will be copied..." );
            spaces = store.getSpaces();
        } else {
            final File file = new File( this.spaceListFilePath );
            if (!file.exists()) {
                throw new FileNotFoundException( "The spaces list file does not exist at " + this.spaceListFilePath );
            }

            spaces = new LinkedList<>();
            BufferedReader is = new BufferedReader( new InputStreamReader( IOUtil.getFileStream( file ) ) );
            String line = null;
            while ((line = is.readLine()) != null) {
                final String spaceId = line.trim();
                spaces.add( spaceId );
            }
        }

        LOGGER.info( "Ready to copy " + spaces.size() + " spaces..." );
        for (String spaceId : spaces) {
            doCopy( store, spaceId );
        }
        LOGGER.info( "Copy Content Tool process complete." );
    }

    protected void doCopy(ContentStore store,
                          String spaceId)
        throws ContentStoreException {

        final Pattern inputSpacePattern = Pattern.compile( inputSpaceRegex );
        final Matcher matcher = inputSpacePattern.matcher( spaceId );
        if (!matcher.find()) {
            LOGGER.info(
                "Space {} does not match the input space regular expression: \"{}\". Skipping.",
                spaceId, inputSpaceRegex );
            return;
        }

        String destinationSpaceId = destinationSpaceFormat;

        for (int i = 0; i <= matcher.groupCount(); i++) {
            destinationSpaceId = destinationSpaceId.replace( "${" + i + "}", matcher.group( i ) );
        }

        LOGGER.info( "Beginning copy of contents of {} to {}", spaceId, destinationSpaceId );

        if (!store.spaceExists( destinationSpaceId )) {
            if (dryRun) {
                LOGGER.info( "DRY RUN: destination space to be created: {}", destinationSpaceId );
            } else {
                LOGGER.info( "Creating space if does not already exist: {}", destinationSpaceId );
                store.createSpace( destinationSpaceId );
                LOGGER.info( "Space created: {}", destinationSpaceId );
            }
        } else {
            LOGGER.info( "Space already exists - no space created: {}", destinationSpaceId );
        }

        Iterator<String> contentIterator = store.getSpaceContents( spaceId );
        while (contentIterator.hasNext()) {
            String contentId = contentIterator.next();
            String destinationContentId = destinationContentFormat;
            destinationContentId = destinationContentId.replace( "${contentId}", contentId );
            for (int i = 1; i < matcher.groupCount(); i++) {
                destinationContentId = destinationContentId.replace( "${" + i + "}", matcher.group( i ) );
            }

            String message = MessageFormat.format( "Copying {0} from {1} to {2} in {3}",
                                                   contentId, spaceId, destinationContentId, destinationSpaceId );
            if (this.dryRun) {
                LOGGER.info( "DRY RUN -- NO COPY : {}", message );
            } else {
                LOGGER.info( message );
                store.copyContent( spaceId, contentId, destinationSpaceId, destinationContentId );
                LOGGER.info( "Content successfully copied: {} in {} was copied to {} in {}",
                             contentId, spaceId, destinationContentId, destinationSpaceId );

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
            new Option( "s", "space-list", true,
                        "the path to the file containing a list of spaces to be copied" );
        spaceFileOption.setRequired( false );
        cmdOptions.addOption( spaceFileOption );

        Option hostOption =
            new Option( "h", "host", true,
                        "the host address of the DuraCloud DuraStore application" );
        hostOption.setRequired( true );
        cmdOptions.addOption( hostOption );

        Option portOption =
            new Option( "t", "port", true,
                        "the port of the DuraCloud DuraStore application " +
                        "(optional, default value is " + DEFAULT_PORT + ")" );
        portOption.setRequired( false );
        cmdOptions.addOption( portOption );

        Option usernameOption =
            new Option( "u", "username", true,
                        "the username necessary to perform writes to DuraStore" );
        usernameOption.setRequired( true );
        cmdOptions.addOption( usernameOption );

        Option passwordOption =
            new Option( "p", "password", true,
                        "the password necessary to perform writes to DuraStore" );
        passwordOption.setRequired( true );
        cmdOptions.addOption( passwordOption );

        Option storeIdOption =
            new Option( "i", "store-id", true,
                        "the ID of the store (optional)" );
        storeIdOption.setRequired( false );
        cmdOptions.addOption( storeIdOption );

        Option dryRunOption =
            new Option( "d", "dry-run", false,
                        "designate this execution as a dry run, no changes " +
                        "will be made, but output will indicate what would have happened" );
        dryRunOption.setRequired( false );
        cmdOptions.addOption( dryRunOption );

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse( cmdOptions, args );
        } catch (ParseException e) {
            LOGGER.info( e.getMessage() );
            usage();
        }

        String host = cmd.getOptionValue( "h" );
        String username = cmd.getOptionValue( "u" );
        String password = cmd.getOptionValue( "p" );
        String storeId = cmd.getOptionValue( "i" );
        String spaceListFilePath = cmd.getOptionValue( "s" );

        String port = cmd.getOptionValue( "t" );
        if (port == null || port.equals( "" )) {
            port = DEFAULT_PORT;
        }

        boolean dryRun = false;
        if (cmd.hasOption( "d" )) {
            dryRun = true;
        }

        CopyContentTool tool =
            new CopyContentTool( host, port, username,
                                 password, storeId, spaceListFilePath,
                                 dryRun );
        tool.run();
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "Running the copy-content-tool", cmdOptions );
        System.exit( 1 );
    }

}