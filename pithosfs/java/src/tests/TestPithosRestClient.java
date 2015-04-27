package tests;

import gr.grnet.escience.commons.PithosSerializer;
import gr.grnet.escience.commons.Utils;
import gr.grnet.escience.fs.pithos.PithosBlock;
import gr.grnet.escience.fs.pithos.PithosFileSystem;
import gr.grnet.escience.fs.pithos.PithosObject;
import gr.grnet.escience.pithos.rest.HadoopPithosConnector;
import gr.grnet.escience.pithos.rest.PithosResponse;
import gr.grnet.escience.pithos.rest.PithosResponseFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.AfterClass;

public class TestPithosRestClient {
    private static final String PITHOS_STORAGE_SYSTEM_URL = "https://pithos.okeanos.grnet.gr/v1";
    private static final String UUID = "ec567bea-4fa2-433d-9935-261a0867ec60";
    private static final String TOKEN = "SFy6ATmUS2cdkbJZkwTDs_cujtFQ87LOCKiLIQiML3g";
    private static final String PITHOS_CONTAINER = "";
    private static final String PITHOS_FILE_TO_DOWNLOAD = "tests/newPithosObjectData.txt";
    private static final String PITHOS_FILE_TO_DOWNLOAD_BLOCK = "tests/newPithosObjectData.txt";
    private static final String PITHOS_FILE_TO_DOWNLOAD_DIR_NAME = "tests/";
    private static final long OFFSET = 5;
    private static final String LOCAL_SOURCE_FILE_TO_UPLOAD = "testOutput.txt";
    private static final String PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM = "tests/newPithosObjectData.txt";
    private static final String DUMMY_BLOCK_DATA = "TEST DATA";
    private static PithosResponse pithosResponse;
    private static Collection<String> object_block_hashes;
    private static HadoopPithosConnector hdconnector;
    private static final Utils util = new Utils();
    private static final String BIG_BLOCK_FILE = "bigBlockFile.txt";
    private static final int SIZE_OF_BIG_BLOCK_FILE = 9437184;

    @BeforeClass
    public static void createHdConnector() {
        // - CREATE HADOOP CONNECTOR INSTANCE
        hdconnector = new HadoopPithosConnector(PITHOS_STORAGE_SYSTEM_URL,
                TOKEN, UUID);
        PithosFileSystem.setHadoopPithosConnector(hdconnector);
    }

    @Test
    public void testGet_Container_Info() {
        // - GET CONTAINER INFORMATION
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET CONTAINER INFO");
        System.out
                .println("---------------------------------------------------------------------");
        pithosResponse = hdconnector.getContainerInfo(PITHOS_CONTAINER);
        System.out.println(pithosResponse.toString());
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Container_File_List() {
        // - GET THE FILE STATUS OF A SELECTED CONTAINER
        System.out
                .println("---------------------------------------------------------------------");
        // - Printout the name of the default container else the given name
        if (PITHOS_CONTAINER.isEmpty()) {
            System.out
                    .println("GET FILE LIST OF THE CONTAINER: [CONTAINER:<pithos>]");
        } else {
            System.out.println("GET FILE LIST OF THE CONTAINER: [CONTAINER:<"
                    + PITHOS_CONTAINER + ">]");
        }
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println(hdconnector.getFileList(PITHOS_CONTAINER));
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Metadata() {
        // - GET METADATA OF A SPECIFIC OBJECT
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT METADATA: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        pithosResponse = hdconnector.getPithosObjectMetaData(PITHOS_CONTAINER,
                PITHOS_FILE_TO_DOWNLOAD, PithosResponseFormat.JSON);
        System.out.println(pithosResponse.toString());
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Size() {
        // - GET OBJECT ACTUAL SIZE
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT SIZE: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        long objectSize = hdconnector.getPithosObjectSize(PITHOS_CONTAINER,
                PITHOS_FILE_TO_DOWNLOAD);
        System.out.println("Requested Object Size: " + objectSize + " Bytes");
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object() {
        // - GET AND STORE THE ACTUAL OBJECT AS A FILE
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS ACTUAL OBJECT: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD
                + ">] and STORE IT AS: <"
                + PITHOS_FILE_TO_DOWNLOAD.substring(PITHOS_FILE_TO_DOWNLOAD
                        .lastIndexOf("/") + 1) + ">");
        System.out
                .println("---------------------------------------------------------------------");
        File pithosActualObject = hdconnector.retrievePithosObject(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD, "data.txt");
        System.out.println("File name: " + pithosActualObject.getName());
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Block_Hashes() {
        // - GET OBJECT HASHES
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT BLOCK HASHES: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        object_block_hashes = hdconnector.getPithosObjectBlockHashes(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD);
        System.out.println("Block Hashes: " + object_block_hashes);
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Block_Default_Size() {
        // -GET BLOCK DEFAULT SIZE
        System.out
                .println("---------------------------------------------------------------------");
        System.out
                .println("GET PITHOS CONTAINER BLOCK DEFAULT SIZE: [CONTAINER:<pithos>]");
        System.out
                .println("---------------------------------------------------------------------");
        long blocksDefaultSize = hdconnector
                .getPithosBlockDefaultSize(PITHOS_CONTAINER);
        System.out.println("Container block defaut size: " + blocksDefaultSize
                + " Bytes");
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Blocks_Number() {
        // - GET THE NUMBER OF THE BLOCKS THAT COMPRISE A PITHOS OBJECT
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT #BLOCKS: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        int blocksNum = hdconnector.getPithosObjectBlocksNumber(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD);
        System.out.println("Object <" + PITHOS_FILE_TO_DOWNLOAD
                + "> is comprised by: " + blocksNum + " Blocks");
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Block_Size() {
        // - GET OBJECT CURRENT BLOCK SIZE, IN CASE IT IS STORED WITH DIFFERENT
        // POLICIES THAT THE DEFAULTS
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT BLOCK CURRENT SIZE: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        long blockSize = hdconnector.getPithosObjectBlockSize(PITHOS_CONTAINER,
                PITHOS_FILE_TO_DOWNLOAD);
        System.out.println("Current object - Block Size: " + blockSize
                + " Bytes");
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Block() {
        // - GET OBJECT BLOCK BY HASH
        // - Get a block hash of the previously requested object
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("GET PITHOS OBJECT ACTUAL BLOCK: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        String block_hash = "";
        int block_counter = 1;
        // - local loop to get the corresponding hash
        for (String hash : object_block_hashes) {
            // - Get the hash of the second block
            if (block_counter == 1) {
                block_hash = hash;
                break;
            }
            block_counter++;
        }

        // - Get the pithos block
        PithosBlock block = hdconnector.retrievePithosBlock(PITHOS_CONTAINER,
                PITHOS_FILE_TO_DOWNLOAD, block_hash);
        System.out.println(block.toString());
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testGet_Pithos_Object_Block_All() {
        // - GET OBJECT ALL BLOCKS
        System.out
                .println("---------------------------------------------------------------------");
        System.out
                .println("GET PITHOS OBJECT ALL ACTUAL BLOCKS WITH HASH & SIZE: [OBJECT:<"
                        + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        PithosBlock[] blocks = hdconnector.retrievePithosObjectBlocks(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD);
        System.out.println("Object <" + PITHOS_FILE_TO_DOWNLOAD
                + "> is comprised by '" + blocks.length + "' blocks:\n");
        // - Iterate on blocks
        for (int blockCounter = 0; blockCounter < blocks.length; blockCounter++) {
            System.out.println("\t- " + blocks[blockCounter].toString());
        }
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testRead_Pithos_Object() throws IOException {
        // - READ PITHOS OBJECT: ESSENTIALLY CREATES INPUTSTREAM FOR A PITHOS
        // OBJECT
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("STREAM PITHOS OBJECT ACTUAL DATA: [OBJECT:<"
                + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");
        InputStream objectInputStream = hdconnector.pithosObjectInputStream(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD);
        if (objectInputStream != null) {
            System.out.println("Available data in object inputstream : "
                    + objectInputStream.available() + " Bytes");
        } else {
            System.err.println("REQUESTED: [OBJECT:<" + PITHOS_FILE_TO_DOWNLOAD
                    + ">] NOT FOUND");
        }
        System.out
                .println("---------------------------------------------------------------------\n");

    }

    @Test
    public void testPithos_Object_Block_InputStream_With_Offset()
            throws IOException {

        // - Local parameters
        String BLOCK_HASH = "7857f68ca749fa9e3493b2e1d78e23ae1fa2bf4a81eb846d9073bd17ce9092af";

        // - READ PITHOS OBJECT BLOCK: ESSENTIALLY CREATES INPUTSTREAM FOR A
        // PITHOS OBJECT BLOCK REQUESTED BY IT'S HASH
        // - Get a block hash of the previously requested object
        System.out
                .println("---------------------------------------------------------------------");
        System.out
                .println("SEEK INTO PITHOS BLOCK DATA: [BLOCK PART OF OBJECT:<"
                        + PITHOS_FILE_TO_DOWNLOAD + ">]");
        System.out
                .println("---------------------------------------------------------------------");

        File objectBlockInputStream = hdconnector.pithosBlockInputStream("",
                PITHOS_FILE_TO_DOWNLOAD, BLOCK_HASH, OFFSET);
        if (objectBlockInputStream != null) {
            System.out.println("Available data in block inputstream : "
                    + objectBlockInputStream.length() + " Bytes");
        } else {
            System.out.println("REQUESTED BLOCK WITH HASH:<" + BLOCK_HASH
                    + ">] NOT FOUND.");
        }
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testStore_File_To_Pithos() throws IOException {
        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("STORE ACTUAL FILE: <" + LOCAL_SOURCE_FILE_TO_UPLOAD
                + "> TO PITHOS STORAGE SYSTEM AS OBJECT <"
                + PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM + ">");
        System.out
                .println("---------------------------------------------------------------------");
        String response = hdconnector.uploadFileToPithos("",
                LOCAL_SOURCE_FILE_TO_UPLOAD);
        System.out.println("RESPONSE FROM PITHOS: " + response);
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testStore_Object_To_Pithos() throws IOException {
        // - Create Pithos Object instance
        PithosObject pithosObj = new PithosObject(
                PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM, null);

        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("STORE ACTUAL OBJECT: <" + pithosObj.getName()
                + "> TO PITHOS STORAGE SYSTEM");
        System.out
                .println("---------------------------------------------------------------------");
        String response = hdconnector.storePithosObject(PITHOS_CONTAINER,
                pithosObj);
        System.out.println("RESPONSE FROM PITHOS: " + response);
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testAppend_Pithos_Small_Block() throws IOException,
            NoSuchAlgorithmException {

        // - Local parameters
        String BLOCK_HASH;
        BLOCK_HASH = util.computeHash(DUMMY_BLOCK_DATA.getBytes(), "SHA-256");

        System.out.println("GENERATED HASH: " + BLOCK_HASH);

        // - Create Pithos Object instance
        byte[] toBeSent = DUMMY_BLOCK_DATA.getBytes();
        PithosBlock pithosBlock = new PithosBlock(BLOCK_HASH, toBeSent.length,
                toBeSent);

        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("APPEND BLOCK: <" + pithosBlock.getBlockHash()
                + "> TO PITHOS OBJECT <" + PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM
                + ">");
        System.out
                .println("---------------------------------------------------------------------");
        String response = hdconnector.appendPithosBlock(PITHOS_CONTAINER,
                PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM, pithosBlock);
        System.out.println("RESPONSE FROM PITHOS: " + response);
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testAppend_Pithos_Big_Block() throws IOException,
            NoSuchAlgorithmException {

        // - Local parameters
        String BLOCK_HASH = null;
        File bigBlock = new File(BIG_BLOCK_FILE);
        Writer output = null;
        try {
            output = new FileWriter(bigBlock);
            while (bigBlock.length() < SIZE_OF_BIG_BLOCK_FILE) {
                output.write("I am nothing but a test file");
            }

        } finally {
            if (output != null) {
                output.close();

            }
        }
        // - Load file bytes
        byte[] bigBlockData = PithosSerializer.serializeFile(bigBlock);

        // - Generate HASH CODE
        BLOCK_HASH = util.computeHash(bigBlockData, "SHA-256");

        // - Create Pithos Object instance
        PithosBlock pithosBlock = new PithosBlock(BLOCK_HASH,
                bigBlockData.length, bigBlockData);

        System.out
                .println("---------------------------------------------------------------------");
        System.out.println("APPEND BLOCK: <" + pithosBlock.getBlockHash()
                + "> TO PITHOS OBJECT <" + PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM
                + ">");
        System.out
                .println("---------------------------------------------------------------------");
        String response = hdconnector.appendPithosBlock(PITHOS_CONTAINER,
                PITHOS_OBJECT_NAME_TO_OUTPUTSTREAM, pithosBlock);
        System.out.println("RESPONSE FROM PITHOS: " + response);
        System.out
                .println("---------------------------------------------------------------------\n");
    }

    @Test
    public void testRead_Pithos_Object_Block_Sizes() throws IOException {
        // - READ PITHOS OBJECT BLOCK: ESSENTIALLY CREATES INPUTSTREAM FOR A
        // PITHOS OBJECT BLOCK REQUESTED BY IT'S HASH
        // - Get a block hash of the previously requested object

        long size = 0;
        object_block_hashes = hdconnector.getPithosObjectBlockHashes(
                PITHOS_CONTAINER, PITHOS_FILE_TO_DOWNLOAD_BLOCK);
        // - local loop to get the corresponding hash
        Object[] blockHashesArray = new Object[object_block_hashes.size()];
        String currentBlockHash;
        blockHashesArray = object_block_hashes.toArray();
        for (int i = 0; i < blockHashesArray.length; i++) {
            currentBlockHash = blockHashesArray[i].toString();
            size = hdconnector.retrievePithosBlock(PITHOS_CONTAINER,
                    PITHOS_FILE_TO_DOWNLOAD_BLOCK, currentBlockHash)
                    .getBlockLength();
            System.out.println("block length:" + size);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException {
        File testFile = new File(PITHOS_FILE_TO_DOWNLOAD.replace(
                PITHOS_FILE_TO_DOWNLOAD_DIR_NAME, ""));
        File bigBlock = new File(BIG_BLOCK_FILE);
        bigBlock.deleteOnExit();
        testFile.deleteOnExit();
    }

    /**
     * @param args
     * @throws IOException
     * 
     */
    public static void main(String[] args) throws IOException {
        TestPithosRestClient client = new TestPithosRestClient();

        TestPithosRestClient.createHdConnector();
        client.testGet_Container_Info();
        client.testGet_Container_File_List();
        client.testGet_Pithos_Object_Metadata();
        client.testGet_Pithos_Object_Size();
        client.testGet_Pithos_Object();
        client.testGet_Pithos_Object_Block_Hashes();
        client.testGet_Pithos_Object_Block_Default_Size();
        client.testGet_Pithos_Object_Blocks_Number();
        client.testGet_Pithos_Object_Block();
        client.testGet_Pithos_Object_Block_All();
        client.testRead_Pithos_Object();
        client.testPithos_Object_Block_InputStream_With_Offset();
        client.testStore_File_To_Pithos();
        client.testStore_Object_To_Pithos();
        try {
            client.testAppend_Pithos_Small_Block();
            client.testAppend_Pithos_Big_Block();
        } catch (NoSuchAlgorithmException e) {
            util.dbgPrint(e.getMessage(), e);
        }
        TestPithosRestClient.tearDown();

    }

}
