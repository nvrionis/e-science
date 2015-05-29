package gr.grnet.escience.pithos.rest;

import gr.grnet.escience.commons.LoggerServer;
import gr.grnet.escience.commons.PithosSerializer;
import gr.grnet.escience.commons.Utils;
import gr.grnet.escience.fs.pithos.PithosBlock;
import gr.grnet.escience.fs.pithos.PithosInputStream;
import gr.grnet.escience.fs.pithos.PithosObject;
import gr.grnet.escience.fs.pithos.PithosPath;
import gr.grnet.escience.fs.pithos.PithosSystemStore;
import gr.grnet.escience.pithos.restapi.PithosRESTAPI;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;

import com.google.gson.Gson;

/***
 * This class extends Pithos REST API that is implemented by grnet and supports
 * the implementation of particular methods for the interaction between hadoop
 * and Pithos Storage System.
 * 
 * {@link:
 * https://www.synnefo.org/docs/synnefo/latest/object-api-guide.html#object
 * -level}
 * 
 * @author dkelaidonis
 * @version 0.1
 * @since March, 2015
 */

public class HadoopPithosConnector extends PithosRESTAPI implements
        PithosSystemStore {

    private static final long serialVersionUID = 1L;
    private transient LoggerServer loggerServer = null;
    private transient PithosRequest request;
    private transient PithosResponse response;
    private transient Object srcFile2bUploaded;
    private File temp;
    private File block_data;
    private transient InputStream pithosFileInputStream;
    private String objectDataContent;
    private String responseStr;
    private transient PithosPath path;
    private transient Thread loggerThread = null;
    private long[] range = { 0, 0 };
    // - Create
    private long current_size = 0;
    private transient Map<String, List<String>> response_data = null;
    private transient PithosResponseHashmap hashMapResp = null;
    private long object_total_size = 0;
    private long block_size = 0;
    private int object_blocks_number = 0;
    private long[] block_bytes_range = null;
    private transient Collection<String> object_block_hashes = null;
    int block_location_pointer_counter = 1;
    int block_location_pointer = 0;
    private transient PithosBlock resultPithosBlock = null;
    private transient PithosBlock[] blocks = null;
    private transient PithosResponse resp = null;
    private String hashAlgo = null;
    private transient FSDataInputStream fsDataInputStream = null;
    private transient PithosInputStream pithosInputStream = null;
    private File pithosBlockData = null;
    private File tmpFile2bUploaded = null;
    private String contentLength = null;

    /********************************************************
     * (PITHOS <--> HADOOP): ANSTRACT METHODS THAT SUPPORT THE INTRERACTION
     * BETWEEN PITHOS AND HADOOP
     ********************************************************/
    /*****
     * Constructor
     */
    public HadoopPithosConnector(String pithosUrl, String pithosToken,
            String uuid) {
        // - implement aPithos RESTAPI instance
        // TODO: Refactor to use org.apache.hadoop.conf.Configuration
        // and pass the conf object from PithosFileSystem instead of option
        // literals
        super(pithosUrl, pithosToken, uuid);

        // - Perform additional check for unused references
        //System.gc();

        // - Initialize the loggerServer
        loggerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                loggerServer = new LoggerServer();
            }
        });
        // - Start logger into a separated thread
        loggerThread.start();

    }

    /***
     * Pithos request
     */
    private PithosRequest getPithosRequest() {
        return request;
    }

    private void setPithosRequest(PithosRequest request) {
        this.request = request;
    }

    /***
     * Pithos response
     */
    private PithosResponse getPithosResponse() {
        return response;
    }

    private void setPithosResponse(PithosResponse response) {
        this.response = response;
    }

    /**
     * Manage Blocks
     */
    private long[] bytesRange(long object_total_size, long block_size,
            int blocks_number, int block_pointer) {
        // - Initialize a long array that will keep 2 values; one for the start
        // of the range and one for the stop of the range of the bytes
        range = null;
        range = new long[2];
        // - Create
        current_size = 0;

        // - Check if there are more than one blocks
        if (blocks_number > 1) {
            // - if the requested block is the first one
            if (block_pointer == 1) {
                // - Get range start
                range[0] = current_size;
                // - Get range stop
                range[1] = block_size - 1;
            }
            // - if the requested block is the last one
            else if (block_pointer == blocks_number) {
                int previous_blocks = blocks_number - 1;
                long previous_size = block_size * previous_blocks;
                long last_block_size = object_total_size - previous_size;
                // - Get range start
                range[0] = (object_total_size - last_block_size);

                // - Get range stop
                range[1] = object_total_size - 1;

            } else {
                // - Any intermediate block
                for (int i = 1; i <= blocks_number; i++) {
                    // - if the current block is the requested one
                    if (i == block_pointer) {
                        // - Get range start
                        range[0] = current_size;

                        // - Get range stop
                        range[1] = range[0] + block_size - 1;

                        // - stop the loop
                        break;
                    }

                    current_size = (current_size + block_size);
                }
            }
        } else {
            // - Get range start
            range[0] = 0;
            // - Get range stop
            range[1] = object_total_size - 1;
        }

        // - Return the table
        return range;
    }

    /********************************************************
     * (PITHOS --> HADOOP): GET / STREAM DATA FROM PITHOS
     ********************************************************/
    @Override
    public PithosResponse getContainerInfo(String pithos_container) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Create Response instance
        setPithosResponse(new PithosResponse());

        // - Read meta-data and add the data on the Pithos Response
        try {
            // - If container argument is empty the initialize it with the
            // default value
            if (pithos_container.equals("")) {
                pithos_container = "pithos";
            }

            // - Add data from pithos response on the corresponding java object
            getPithosResponse().setResponseData(
                    retrieve_container_info(pithos_container,
                            getPithosRequest().getRequestParameters(),
                            getPithosRequest().getRequestHeaders()));

        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }

        // - Return the response data as String
        return getPithosResponse();
    }

    @Override
    public String getFileList(String pithos_container) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Create Response instance
        setPithosResponse(new PithosResponse());
        response_data = null;
        // - Read meta-data and add the data on the Pithos Response
        try {
            // - Perform action by using Pithos REST API method
            // - Return the response data as String
            return list_container_objects(pithos_container, getPithosRequest()
                    .getRequestParameters(), getPithosRequest()
                    .getRequestHeaders());
        } catch (IOException e) {
            // - Return the exception message as String
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public File retrievePithosObject(String pithos_container,
            String object_location, String destination_file) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");

        // - Read data object
        try {

            return (File) read_object_data(object_location, pithos_container,
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public long getPithosObjectSize(String pithos_container,
            String object_location) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        getPithosRequest().getRequestParameters().put("hashmap", "True");

        hashMapResp = null;

        // - Read data object
        try {
            // - Get response data in json format
            // String json = (String) read_object_data(object_location,
            // pithos_container,
            // getPithosRequest().getRequestParameters(),
            // getPithosRequest().getRequestHeaders());

            // -Serialize json response into Java object PithosResponseHashmap
            hashMapResp = (new Gson()).fromJson(
                    (String) read_object_data(object_location,
                            pithos_container, getPithosRequest()
                                    .getRequestParameters(), getPithosRequest()
                                    .getRequestHeaders()),
                    PithosResponseHashmap.class);

            // - Return the required value
            if (hashMapResp != null) {
                return Long.parseLong(hashMapResp.getObjectSize());
            } else {
                return -1;
            }
        } catch (IOException e) {
            hashMapResp = null;
            Utils.dbgPrint(e.getMessage(), e);
            return -1;
        }
    }

    @Override
    public PithosBlock retrievePithosBlock(String pithos_container,
            String object_location, String block_hash) {

        // - Get required info for the object and the block
        object_total_size = getPithosObjectSize(pithos_container,
                object_location);
        block_size = getPithosObjectBlockSize(pithos_container, object_location);
        object_blocks_number = getPithosObjectBlocksNumber(pithos_container,
                object_location);

        object_block_hashes = getPithosObjectBlockHashes(pithos_container,
                object_location);

        // - Iterate on available hashes
        block_location_pointer_counter = 1;
        block_location_pointer = 0;

        for (String hash : object_block_hashes) {
            // - If the hash is the requested hash
            if (hash.equals(block_hash)) {
                // - Get the location of the block
                block_location_pointer = block_location_pointer_counter;
                break;
            }
            // - Move the pointer one step forward
            block_location_pointer_counter++;
        }

        // - Get the Range of the byte for the requested block
        block_bytes_range = null;
        block_bytes_range = bytesRange(object_total_size, block_size,
                object_blocks_number, block_location_pointer);

        // - Create byte array for the object
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        // - Add requested parameter for the range
        // - If it is not requested the last block, the add specific range
        if (block_bytes_range[1] != object_total_size - 1) {
            getPithosRequest().getRequestHeaders().put(
                    "Range",
                    "bytes=" + block_bytes_range[0] + "-"
                            + block_bytes_range[1]);
        } else {
            getPithosRequest().getRequestHeaders().put("Range",
                    "bytes=" + block_bytes_range[0] + "-");
        }

        // - Read data object
        try {
            // - Get the chunk of the pithos object as a file
            block_data = (File) read_object_data(object_location,
                    pithos_container,
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());

            resultPithosBlock = new PithosBlock(block_hash,
                    block_data.length(),
                    PithosSerializer.serializeFile(block_data));

            // - Return the created pithos object
            return resultPithosBlock;
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            if (block_data != null) {
                block_data = null;
            }
            if (resultPithosBlock != null) {
                resultPithosBlock = null;
            }
        }
    }

    @Override
    public int getPithosObjectBlocksNumber(String pithos_container,
            String object_location) {

        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        getPithosRequest().getRequestParameters().put("hashmap", "True");

        // - Read data object
        try {
            // - Get response data in json format
            // String json = (String) read_object_data(object_location,
            // pithos_container,
            // getPithosRequest().getRequestParameters(),
            // getPithosRequest().getRequestHeaders());
            // -Serialize json response into Java object PithosResponseHashmap
            hashMapResp = (new Gson()).fromJson(
                    (String) read_object_data(object_location,
                            pithos_container, getPithosRequest()
                                    .getRequestParameters(), getPithosRequest()
                                    .getRequestHeaders()),
                    PithosResponseHashmap.class);
            // - Return the required value
            return hashMapResp.getBlockHashes().size();
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return -1;
        } finally {
            if (hashMapResp != null) {
                hashMapResp = null;
            }
        }

    }

    @Override
    public Collection<String> getPithosObjectBlockHashes(
            String pithos_container, String object_location) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        getPithosRequest().getRequestParameters().put("hashmap", "True");

        // - Read data object
        try {
            // - Get response data in json format
            // String json = (String) read_object_data(object_location,
            // pithos_container,
            // getPithosRequest().getRequestParameters(),
            // getPithosRequest().getRequestHeaders());
            // -Serialize json response into Java object PithosResponseHashmap
            hashMapResp = (new Gson()).fromJson(
                    (String) read_object_data(object_location,
                            pithos_container, getPithosRequest()
                                    .getRequestParameters(), getPithosRequest()
                                    .getRequestHeaders()),
                    PithosResponseHashmap.class);
            // - Return the required value
            return hashMapResp.getBlockHashes();
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            if (hashMapResp != null) {
                hashMapResp = null;
            }
        }
    }

    @Override
    public PithosBlock[] retrievePithosObjectBlocks(String pithos_container,
            String object_location) {
        // - Create local blocks Array
        blocks = null;

        // - Get the hashes of the blocks for the requested object
        Collection<String> block_hashes = getPithosObjectBlockHashes(
                pithos_container, object_location);

        // - Initialize the local blocks array
        blocks = new PithosBlock[block_hashes.size()];

        // - Get and store on array all the available blocks
        int block_counter = 0;
        for (String hash : block_hashes) {
            blocks[block_counter] = retrievePithosBlock(pithos_container,
                    object_location, hash);

            // - Next block
            block_counter++;
        }

        return blocks;
    }

    @Override
    public long getPithosObjectBlockSize(String pithos_container,
            String object_location) {

        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        getPithosRequest().getRequestParameters().put("hashmap", "True");

        // - Read data object
        try {
            // - Get response data in json format
            // String json = (String) read_object_data(object_location,
            // pithos_container,
            // getPithosRequest().getRequestParameters(),
            // getPithosRequest().getRequestHeaders());
            // System.out.println(json);
            // -Serialize json response into Java object PithosResponseHashmap
            hashMapResp = (new Gson()).fromJson(
                    (String) read_object_data(object_location,
                            pithos_container, getPithosRequest()
                                    .getRequestParameters(), getPithosRequest()
                                    .getRequestHeaders()),
                    PithosResponseHashmap.class);
            // - Return the required value
            return Long.parseLong(hashMapResp.getBlockSize());
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return -1;
        } finally {
            if (hashMapResp != null) {
                hashMapResp = null;
            }
        }

    }

    @Override
    public long getPithosBlockDefaultSize(String pithos_container) {
        // - Create response object
        PithosResponse resp = (new Gson()).fromJson(
                (new Gson()).toJson(getContainerInfo(pithos_container)),
                PithosResponse.class);

        // - Return the value of the block size
        return Long.parseLong(resp.getResponseData()
                .get("X-Container-Block-Size").get(0));

    }

    @Override
    public String getPithosContainerHashAlgorithm(String pithos_container) {
        // - Create response object
        resp = (new Gson()).fromJson(
                (new Gson()).toJson(getContainerInfo(pithos_container)),
                PithosResponse.class);
        // - Return the name of the hash algorithm
        hashAlgo = resp.getResponseData().get("X-Container-Block-Hash").get(0);
        return Utils.fixPithosHashName(hashAlgo);
    }

    @Override
    public PithosResponse getPithosObjectMetaData(String pithos_container,
            String object_location, PithosResponseFormat format) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters JSON Format
        if (format.equals(PithosResponseFormat.JSON)) {
            getPithosRequest().getRequestParameters().put("format", "json");
        } else {
            getPithosRequest().getRequestParameters().put("format", "xml");
        }
        // - Get the actual object
        getPithosRequest().getRequestParameters().put("hashmap", "True");

        // - Create Response instance
        setPithosResponse(new PithosResponse());

        // - Read meta-data and add the data on the Pithos Response
        try {
            // - Perform action by using Pithos REST API method
            response_data = null;
            response_data = retrieve_object_metadata(object_location,
                    pithos_container,
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());

            // - Add data from pithos response on the corresponding java object
            getPithosResponse().setResponseData(response_data);

        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }

        // - Return Pithos Response object as the result
        return getPithosResponse();
    }

    @Override
    public FSDataInputStream pithosObjectInputStream(String pithos_container,
            String object_location) {

        // - Release potential unused data
        fsDataInputStream = null;
        pithosInputStream = null;

        // - Create input stream for pithos
        try {

            pithosInputStream = new PithosInputStream(pithos_container,
                    object_location);

            fsDataInputStream = new FSDataInputStream(pithosInputStream);

            // - Return the input stream wrapped into a FSDataINputStream
            return fsDataInputStream;
        } catch (Exception e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            // - Release potential unused data
            fsDataInputStream = null;
            pithosInputStream = null;
        }

    }

    @Override
    public FSDataInputStream pithosBlockInputStream(String pithos_container,
            String object_location, String block_hash) {
        // - Create input stream for Pithos
        try {
            if (pithosFileInputStream != null) {
                pithosFileInputStream.close();
                pithosFileInputStream = null;
            }

            // - Get the file object from pithos
            resultPithosBlock = retrievePithosBlock(pithos_container,
                    object_location, block_hash);

            // - Add File data to the input stream
            pithosBlockData = PithosSerializer
                    .deserializeFile(resultPithosBlock.getBlockData());

            // - Create File input stream
            pithosFileInputStream = new FileInputStream(pithosBlockData);

            // - Return the input stream wrapped into a FSDataINputStream
            // return pithosFileInputStream;
            fsDataInputStream = new FSDataInputStream(pithosFileInputStream);
            return fsDataInputStream;
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            resultPithosBlock = null;
            pithosBlockData = null;
            fsDataInputStream = null;
        }

    }

    @Override
    public File pithosBlockInputStream(String pithos_container,
            String object_location, String block_hash,
            long offsetIntoPithosBlock) {

        // - Get required info for the object and the block
        object_total_size = getPithosObjectSize(pithos_container,
                object_location);
        block_size = getPithosObjectBlockSize(pithos_container, object_location);
        object_blocks_number = getPithosObjectBlocksNumber(pithos_container,
                object_location);

        object_block_hashes = getPithosObjectBlockHashes(pithos_container,
                object_location);

        // - Iterate on available hashes
        block_location_pointer_counter = 1;
        for (String hash : object_block_hashes) {
            // - If the hash is the requested hash
            if (hash.equals(block_hash)) {
                break;
            }
            // - Move the pointer one step forward
            block_location_pointer_counter++;
        }

        System.out
                .println("Object pointer = " + block_location_pointer_counter);

        // - Find the bytes range of the current block
        range = bytesRange(object_total_size, block_size, object_blocks_number,
                block_location_pointer_counter);

        System.out.println("RANGE [" + range[0] + "-" + range[1] + "]");

        // - Check if the requested offset is between the actual range of the
        // block
        if ((offsetIntoPithosBlock >= range[0])
                && (offsetIntoPithosBlock < range[1])) {

            try {
                // - Get the block as file based on the requested offset
                setPithosRequest(new PithosRequest());

                // - Request Parameters
                // - JSON Format
                getPithosRequest().getRequestParameters().put("format", "json");

                // - Add requested parameter for the range
                // - If it is not requested the last block, the add specific
                // range
                getPithosRequest().getRequestHeaders().put("Range",
                        "bytes=" + offsetIntoPithosBlock + "-" + range[1]);

                // - Get the chunk of the pithos object as a file
                block_data = (File) read_object_data(object_location,
                        pithos_container, getPithosRequest()
                                .getRequestParameters(), getPithosRequest()
                                .getRequestHeaders());

                // -Return the actual data of after the block seek
                return block_data;
            } catch (IOException e) {
                Utils.dbgPrint(e.getMessage(), e);
                return null;
            } finally {
                if (block_data != null) {
                    block_data.delete();
                    block_data = null;
                }
            }
        } else {
            System.err
                    .println("The defined offset into seek Pithos Block is out of range...\n\t"
                            + "offset = "
                            + offsetIntoPithosBlock
                            + " | BlockRange["
                            + range[0]
                            + "-"
                            + range[1]
                            + "]");

            return null;
        }

    }

    @Override
    public void deletePithosObject(String pithos_container,
            String object_location) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deletePithosBlock(String block_hash) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean pithosObjectExists(String pithos_container,
            String pithos_object_name) {

        if (getFileList(pithos_container).contains("pithos_object_name")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean pithosObjectBlockExists(String pithos_container,
            String blockHash) {
        // - Get all available object into the container
        // TODO: Get all available objects on the container
        // put them into List<String>
        // for each object use the getPithosObjectBlockHashes(pithos_container,
        // object_location) so as to check if the requested block hash exist
        return false;
    }

    /********************************************************
     * (HADOOP --> PITHOS): POST/PUT STREAM DATA TO PITHOS
     ********************************************************/
    @Override
    public String storePithosObject(String pithos_container,
            PithosObject pithos_object) {
        try {
            // - Create Pithos request
            setPithosRequest(new PithosRequest());

            // - Check if exists and if no, then create it
            if (!getFileList(pithos_container)
                    .contains(pithos_object.getName())) {
                // - Create the file
                createEmptyPithosObject(pithos_container, pithos_object);

                // - This means that the object should be created
                if (pithos_object.getObjectSize() <= 0) {
                    objectDataContent = " ";
                } else {
                    // - Create String from inputstream that corresponds to the
                    // serialized object
                    objectDataContent = PithosSerializer
                            .inputStreamToString(pithos_object.serialize());
                }

                // - Request Parameters
                getPithosRequest().getRequestParameters().put("format", "json");

                // - Request Headers
                getPithosRequest().getRequestHeaders().put("Content-Range",
                        "bytes */*");

                if (pithos_object.getName() != null) {
                    if (!pithos_object.getName().isEmpty()) {
                        return update_append_truncate_object(pithos_container,
                                pithos_object.getName(), objectDataContent,
                                getPithosRequest().getRequestParameters(),
                                getPithosRequest().getRequestHeaders());
                    } else {
                        return "ERROR: Pithos cannot be empty.";
                    }
                } else {
                    return "ERROR: Pithos object must contain a name.";
                }
            } else {
                return "ERROR: Object <" + pithos_object.getName()
                        + "> already exists.";
            }
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;

        }
    }

    @Override
    public String createEmptyPithosObject(String pithos_container,
            PithosObject pithos_object) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Header Parameters
        // - Format of the uploaded file
        getPithosRequest().getRequestHeaders()
                .put("Content-Type", "text/plain");

        try {
            // - Create pithos path
            path = new PithosPath(pithos_container, pithos_object.getName());

            // create a temp file
            temp = File.createTempFile(path.getObjectName(), "");

            // - Get temp file contents into the file that will be uploaded into
            // pithos selected container
            tmpFile2bUploaded = null;
            tmpFile2bUploaded = new File(path.getObjectName());
            temp.renameTo(tmpFile2bUploaded);

            // - Upload file to the root of the selected container
            responseStr = upload_file(tmpFile2bUploaded, null,
                    path.getContainer(), getPithosRequest()
                            .getRequestParameters(), getPithosRequest()
                            .getRequestHeaders());

            // - Check if file should be moved from root pithos to another
            // folder
            if ((!path.getObjectFolderAbsolutePath().isEmpty())) {
                // - If the file is successfully upload to the root of pithos
                // container
                if (responseStr.contains("201")) {
                    return movePithosObjectToFolder(path.getContainer(),
                            tmpFile2bUploaded.getName(),
                            path.getObjectFolderAbsolutePath(), null);
                } else {
                    return "ERROR: Fail to create the object into the requested location";
                }
            } else {
                return responseStr;
            }
        } catch (IOException e) {
            // - Return the exception message as String
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            if (temp != null) {
                temp.delete();
            }
            if (srcFile2bUploaded instanceof File && srcFile2bUploaded != null) {
                ((File) srcFile2bUploaded).delete();
            }
        }
    }

    @Override
    public String movePithosObjectToFolder(String pithosContainer,
            String sourceObject, String targetFolderPath, String targetObject) {
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Header Parameters
        // - Format of the uploaded file
        getPithosRequest().getRequestParameters().put("format", "json");

        // - Check if the folder path is in appropriate format
        if (!targetFolderPath.isEmpty()) {
            if (!targetFolderPath.endsWith("/")) {
                targetFolderPath = targetFolderPath.concat("/");
            }
        }
        String toFilename = null;
        if (targetObject == null || targetObject.isEmpty()) {
            toFilename = targetFolderPath.concat(sourceObject);
        } else {
            toFilename = targetFolderPath.concat(targetObject);
        }
        try {
            // - Post data and get the response
            return move_object(pithosContainer, sourceObject, pithosContainer,
                    toFilename, getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());

        } catch (IOException e) {
            // - Return the exception message as String
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }

    }

    @Override
    public String uploadFileToPithos(String pithosContainer, String sourceFile,
            boolean isDir) {
        isDir = !(!isDir);
        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        String strLength = null;
        try {
            if (isDir) {
                srcFile2bUploaded = sourceFile;
                strLength = "0";
                // - Header Parameters
                // - Format of the uploaded file
                getPithosRequest().getRequestHeaders().put("Content-Type",
                        "application/directory");
            }

            else {
                srcFile2bUploaded = new File(sourceFile);
                // - Header Parameters
                // - Format of the uploaded file
                getPithosRequest().getRequestHeaders().put("Content-Type",
                        "text/plain");
            }

            // - If there is successful renaming of the object into the required
            // name
            // - Post data and get the response
            return upload_file(srcFile2bUploaded, strLength, pithosContainer,
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());
        } catch (IOException e) {
            // - Return the exception message as String
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }

    }

    @Override
    public String appendPithosBlock(String pithos_container,
            String target_object, PithosBlock newPithosBlock) {

        // - Create Pithos request
        setPithosRequest(new PithosRequest());

        // - Request Parameters
        getPithosRequest().getRequestParameters().put("format", "json");

        // - Request Headers
        getPithosRequest().getRequestHeaders().put("Content-Type",
                "application/octet-stream");

        getPithosRequest().getRequestHeaders()
                .put("Content-Range", "bytes */*");

        contentLength = ((Integer) newPithosBlock.getBlockData().length)
                .toString();

        Utils.dbgPrint("appendPithosBlock content-length >", contentLength);
        getPithosRequest().getRequestHeaders().put("Content-Length",
                contentLength);

        getPithosRequest().getRequestHeaders().put("Content-Encoding", "UTF-8");

        try {
            return update_append_truncate_object(pithos_container,
                    target_object, new String(newPithosBlock.getBlockData(),
                            "UTF-8"),
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());
        } catch (UnsupportedEncodingException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } catch (IOException e) {
            // - Return the exception message as String
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        }

    }

    @Override
    public File retrievePithosBlocks(String pithosContainer,
            String targetObject, long targetBlockStart, long targetBlockEnd) {

        setPithosRequest(new PithosRequest());

        // - Request Parameters
        // - JSON Format
        getPithosRequest().getRequestParameters().put("format", "json");
        // - Add requested parameter for the range
        // - If it is not requested the last block, then add specific range
        getPithosRequest().getRequestHeaders().put("Range",
                "bytes=" + targetBlockStart + "-" + targetBlockEnd);

        // - Read data object
        try {
            // - Get the chunk of the pithos object as a file
            block_data = (File) read_object_data(targetObject, pithosContainer,
                    getPithosRequest().getRequestParameters(),
                    getPithosRequest().getRequestHeaders());

            // - Return the created pithos object
            return block_data;
        } catch (IOException e) {
            Utils.dbgPrint(e.getMessage(), e);
            return null;
        } 
    }

    @Override
    public String storePithosBlock(String pithos_container,
            String target_object, PithosBlock pithos_block, File backup_file) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String pithosObjectOutputStream(String pithos_container,
            String object_name, PithosObject pithos_object) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String pithosBlockOutputStream(String pithos_container,
            String target_object, PithosBlock pithos_block) {
        // TODO Auto-generated method stub
        return null;
    }

}
