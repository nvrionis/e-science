package gr.grnet.escience.fs.pithos;

import gr.grnet.escience.commons.Utils;
import gr.grnet.escience.pithos.rest.HadoopPithosConnector;
import gr.grnet.escience.pithos.rest.PithosResponse;
import gr.grnet.escience.pithos.rest.PithosResponseFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * This class implements a custom file system based on FIleSystem class of
 * Hadoop 2.5.2. Essentially the main idea here, respects to the development of
 * a custom File System that will be able to allow the interaction between
 * hadoop and pithos storage system.
 * 
 * @since March, 2015
 * @author eScience Dev Team
 * @version 0.1
 * 
 */
public class PithosFileSystem extends FileSystem {

    private URI uri;
    private static HadoopPithosConnector hadoopPithosConnector;
    private Path workingDir;
    private String pathToString;
    private PithosPath pithosPath;
    private PithosPath commitPithosPath;
    private static String filename;
    private String[] filesList;
    private boolean isDir = false;
    private long length = 0;
    private PithosFileStatus pithosFileStatus;
    private PithosPath srcPiPath = null;
    private PithosPath dstPiPath = null;
    private String srcName = null;
    private String dstName = null;
    private String resp = null;
    private FSDataOutputStream fsDataOutputStreamInstance = null;
    private String urlEsc;
    private PithosResponse metadata = null;
    private String pathEsc = null;
    private String modificationTime = null;
    private ArrayList<FileStatus> results = null;
    private FileStatus fileStatus = null;
    private String[] files = null;
    private FileStatus[] resultsArr = null;
    private PithosOutputStream pithosOutputStreamInstance = null;
    private static final long DEFAULT_HDFS_BLOCK_SIZE = (long)128 * 1024 * 1024;
    private String fromAttemptDirectory = null;
    private String toOutputRootDirectory = null;
    private String[] resultFileName = null;
    private String copyFromFullPath = null;
    private String copyToFullPath = null;
    private FileStatus[] resultFilesList = null;
    private int resultFilesCounter = 0;
    private boolean commitCalled = false;

    public PithosFileSystem() {
    }

    /**
     * @return the instance of hadoop - pithos connector
     */
    public static HadoopPithosConnector getHadoopPithosConnector() {
        return hadoopPithosConnector;
    }

    /**
     * Set the instance of hadoop - pithos connector
     */
    public static void setHadoopPithosConnector(
            HadoopPithosConnector hadoopPithosConnectorIn) {
        PithosFileSystem.hadoopPithosConnector = hadoopPithosConnectorIn;
    }

    @Override
    public String getScheme() {
        Utils.dbgPrint("getScheme >", "pithos");
        return "pithos";
    }

    @Override
    public URI getUri() {
        Utils.dbgPrint("getUri >", uri);
        return uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        Utils.dbgPrint("initialize");
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        Utils.dbgPrint("uri >", this.uri);
        // this.workingDir = new Path("/user", System.getProperty("user.name"));
        setWorkingDirectory(new Path("/user", System.getProperty("user.name")));
        Utils.dbgPrint("workingDir >", getWorkingDirectory());
        Utils.dbgPrint("Create System Store connector");

        if (hadoopPithosConnector == null) {
            setHadoopPithosConnector(new HadoopPithosConnector(
                    conf.get("fs.pithos.url"), conf.get("auth.pithos.token"),
                    conf.get("auth.pithos.uuid")));
        }

    }

    @Override
    public Path getWorkingDirectory() {
        Utils.dbgPrint("getWorkingDirectory", workingDir);
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        workingDir = makeAbsolute(dir);
        Utils.dbgPrint("setWorkingDirectory >", workingDir);
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    /** This optional operation is not yet supported. */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        Utils.dbgPrint("append");
        throw new IOException("Not supported");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {

        // - Initialize & release previously allocated memory
        pithosOutputStreamInstance = null;
        fsDataOutputStreamInstance = null;

        pithosPath = new PithosPath(f);
        Utils.dbgPrint("create > container", pithosPath.getContainer());

        Utils.dbgPrint("create >", f, pithosPath, getHadoopPithosConnector()
                .getPithosBlockDefaultSize(pithosPath.getContainer()),
                bufferSize);

        // - Create empty object on Pithos FS with the given name by using the
        // path
        getHadoopPithosConnector().storePithosObject(pithosPath.getContainer(),
                new PithosObject(new PithosPath(f), null));

        pithosOutputStreamInstance = new PithosOutputStream(getConf(),
                pithosPath, getHadoopPithosConnector()
                        .getPithosBlockDefaultSize(pithosPath.getContainer()),
                1 * 1024 * 1024);

        fsDataOutputStreamInstance = new FSDataOutputStream(
                pithosOutputStreamInstance, statistics);

        return fsDataOutputStreamInstance;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        Utils.dbgPrint("delete > path, recurse ", f, recursive);
        pithosPath = new PithosPath(f);
        resp = getHadoopPithosConnector().deletePithosObject(
                pithosPath.getContainer(), pithosPath.getObjectAbsolutePath());
        if (resp.contains("204")) {
            Utils.dbgPrint("=================================================");
            Utils.dbgPrint("DELETE CALLED - SUCCESSFULLY");
            Utils.dbgPrint("=================================================");

            return true;
        }
        Utils.dbgPrint("=================================================");
        Utils.dbgPrint("DELETE CALLED - FAILED");
        Utils.dbgPrint("=================================================");

        return false;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        // TODO only for testing, should re-use getFileStatus normally
        Utils.dbgPrint("exists", f);
        return super.exists(f);
    }

    @Override
    public PithosFileStatus getFileStatus(Path targetPath) throws IOException {
        Utils.dbgPrint("getFileStatus", "ENTRY");
        Utils.dbgPrint("targetPath >", targetPath);

        // - Process the given path
        pithosPath = new PithosPath(targetPath);
        Utils.dbgPrint("pithosPath >", pithosPath.getObjectAbsolutePath());

        // - Check if it is the final call from outputstream and perform the
        // final action for the result file(s) movement
        if (PithosOutputStream.isClosed() && !isCommitCalled()) {
            // - Set the current path as the one that constitutes the commit
            // directory for Hadoop outpustream
            setCommitPithosPath(pithosPath);

            // - Perform the final commit by moving the result files to the root
            // output folder
            commitFinalResult();
        }

        urlEsc = null;
        try {
            urlEsc = Utils.urlEscape(null, null,
                    pithosPath.getObjectAbsolutePath(), null);
        } catch (URISyntaxException e) {
            Utils.dbgPrint("getFileStatus > invalid pithosPath");
            throw new IOException(e);
        }
        metadata = getHadoopPithosConnector().getPithosObjectMetaData(
                pithosPath.getContainer(), urlEsc, PithosResponseFormat.JSON);
        if (metadata.toString().contains("HTTP/1.1 404 NOT FOUND")) {
            Utils.dbgPrint("File does not exist in Pithos FS.");
            throw new FileNotFoundException("File does not exist in Pithos FS.");
        }
        for (String obj : metadata.getResponseData().keySet()) {
            if (obj != null
                    && (obj.matches("Content-Type") || obj
                            .matches("Content_Type"))) {
                for (String fileType : metadata.getResponseData().get(obj)) {
                    if (fileType.contains("application/directory")
                            || fileType.contains("application/folder")) {
                        isDir = true;
                        break;
                    } else {
                        isDir = false;
                    }
                }
            }
        }
        if (isDir) {
            pithosFileStatus = new PithosFileStatus(true,
                    DEFAULT_HDFS_BLOCK_SIZE, false, targetPath);
        } else {
            for (String obj : metadata.getResponseData().keySet()) {
                if (obj != null && obj.matches("Content-Length")) {
                    for (String lengthStr : metadata.getResponseData().get(obj)) {
                        length = Long.parseLong(lengthStr);
                    }
                }
            }
            modificationTime = metadata.getResponseData().get("Last-Modified")
                    .get(0);

            pithosFileStatus = new PithosFileStatus(length,
                    DEFAULT_HDFS_BLOCK_SIZE, Utils.dateTimeToEpoch(
                            modificationTime, ""), targetPath);
        }
        Utils.dbgPrint("getFileStatus", "EXIT");
        Utils.dbgPrint("pithos_file_status >", pithosFileStatus);

        Utils.dbgPrint("=================================================");
        Utils.dbgPrint("GET FILE STATUS CALLED");
        Utils.dbgPrint("=================================================");

        return pithosFileStatus;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        Utils.dbgPrint("listStatus > path", f);

        filename = "";
        pithosPath = new PithosPath(f);
        pathToString = pithosPath.toString();

        pathToString = pathToString.substring(this.getScheme().toString()
                .concat("://").length());

        Utils.dbgPrint("listStatus > pathToString", pathToString);
        filesList = pathToString.split("/");
        filename = filesList[filesList.length - 1];
        Utils.dbgPrint("listStatus > 1. filename", filename);
        int count = 2;
        while (!filesList[filesList.length - count].equals(pithosPath
                .getContainer())) {
            filename = filesList[filesList.length - count] + "/" + filename;
            count++;
        }
        Utils.dbgPrint("listStatus > 2. filename", filename);
        // results = Collections.synchronizedList(new ArrayList<FileStatus>());
        results = new ArrayList<FileStatus>();

        files = getHadoopPithosConnector().getFileList(
                pithosPath.getContainer()).split("\\r?\\n");
        // - Iterate on available files in the container
        for (int i = 0; i < files.length; i++) {
            String file = files[i].substring(files[i].lastIndexOf("/") + 1);
            files[i] = files[i].substring(0, files[i].length() - file.length());
            if ((filename + "/").equals(files[i])) {
                Path path = new Path("pithos://" + pithosPath.getContainer()
                        + "/" + filename + "/" + file);
                fileStatus = getFileStatus(path);
                results.add(fileStatus);
            }
        }
        // - Return the list of the available files
        resultsArr = new FileStatus[results.size()];
        for (int i = 0; i < results.toArray(resultsArr).length; i++) {
            Utils.dbgPrint("listStatus results >", i, resultsArr[i]);
        }

        Utils.dbgPrint("=================================================");
        Utils.dbgPrint("LIST STATUS CALLED");
        Utils.dbgPrint("=================================================");

        return results.toArray(resultsArr);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Utils.dbgPrint("mkdirs path >", f);
        pithosPath = new PithosPath(f);
        Utils.dbgPrint("mkdirs pithosPath >",
                pithosPath.getObjectFolderAbsolutePath());
        Utils.dbgPrint("mkdirs > uploadFileToPithos > ",
                pithosPath.getContainer(),
                pithosPath.getObjectFolderAbsolutePath(), true);
        resp = getHadoopPithosConnector().uploadFileToPithos(
                pithosPath.getContainer(),
                pithosPath.getObjectFolderAbsolutePath(), true);

        if (resp != null && resp.contains("201")) {
            return true;
        }
        Utils.dbgPrint("mkdirs> response:", resp);
        return false;
    }

    @Override
    public FSDataInputStream open(Path targetFile, int bufferSize)
            throws IOException {
        pithosPath = new PithosPath(targetFile);
        pathEsc = null;
        try {
            pathEsc = Utils.urlEscape(null, null,
                    pithosPath.getObjectAbsolutePath(), null);
        } catch (URISyntaxException e) {
            Utils.dbgPrint("open > invalid targetFile");
            throw new IOException(e);
        }

        return getHadoopPithosConnector().pithosObjectInputStream(
                pithosPath.getContainer(), pathEsc);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        Utils.dbgPrint("rename", src, dst);
        srcPiPath = new PithosPath(src);
        dstPiPath = new PithosPath(dst);
        srcName = srcPiPath.getObjectAbsolutePath();
        dstName = dstPiPath.getObjectAbsolutePath();
        Utils.dbgPrint("rename src, dst", srcName, dstName);
        resp = getHadoopPithosConnector().movePithosObjectToFolder(
                srcPiPath.getContainer(), srcName, "", dstName);
        Utils.dbgPrint("rename resp>", resp);

        Utils.dbgPrint("=================================================");
        Utils.dbgPrint("RENAME CALLED");
        Utils.dbgPrint("=================================================");

        if (resp.contains("201")) {
            return true;
        } else {
            return false;
        }
    }

    /***
     * Additional private methods for the management of the final results move
     * to the root output directory
     * 
     */
    /**
     * 
     * @return: the full path of the temp files during the output-stream example
     */
    private PithosPath getCommitPithosPath() {
        return commitPithosPath;
    }

    private void setCommitPithosPath(PithosPath _path) {
        this.commitPithosPath = _path;
    }

    /**
     * 
     * @return: check if the commit method has been already called, in order to
     *          avoid any potential problems due to the recursive behaviour of
     *          listStatus method
     */
    private boolean isCommitCalled() {
        return commitCalled;
    }

    /**
     * Perform the move of the final result files from the temp files to the
     * root ouput file, and delete the remaining unused temp files
     */
    private void commitFinalResult() {

        // - Avoid the impact due to the recursive behaviour
        this.commitCalled = true;

        // - Get the attempt folder of the output result
        fromAttemptDirectory = getCommitPithosPath().getObjectAbsolutePath();

        // - Get the root folder
        toOutputRootDirectory = getCommitPithosPath().getObjectAbsolutePath()
                .substring(
                        0,
                        getCommitPithosPath().getObjectAbsolutePath().indexOf(
                                "/_temporary"));

        try {
            // - Get the file status by all available files into selected
            // results directory
            resultFilesList = listStatus(getCommitPithosPath()
                    .getPithosFSPath());

            // - Initialize the string array that includes the file names of the
            // result files
            resultFileName = new String[resultFilesList.length];

            // - Iterate on results directory contents
            resultFilesCounter = 0;

            // - Iterate on file status array results so as to get one-by-one
            // the available result file names
            for (FileStatus resultFileFStatus : resultFilesList) {
                Utils.dbgPrint("RESULT FILE INTO ATTEMPT ("
                        + fromAttemptDirectory + ") --> "
                        + resultFileFStatus.getPath().getName());

                // - Get the file name
                resultFileName[resultFilesCounter] = resultFileFStatus
                        .getPath().getName();

                // - increase the counter of the files
                resultFilesCounter++;
            }

        } catch (IOException e) {
            Utils.dbgPrint("commitFinalResult error: ",e);
        }

        // - Iterate on all results files
        for (String resultFile : resultFileName) {
            // - Create the full From --> To paths that will be used by PithosFS
            // methods so as to perform the move of the result files into pithos
            // file storage
            copyFromFullPath = fromAttemptDirectory.concat("/").concat(
                    resultFile);
            copyToFullPath = toOutputRootDirectory.concat("/").concat(
                    resultFile);

            Utils.dbgPrint("=================================================");
            Utils.dbgPrint("MOVE RESULTS FOLDER");
            Utils.dbgPrint("=================================================");
            Utils.dbgPrint("FROM FOLDER --> " + fromAttemptDirectory);
            Utils.dbgPrint("THE FILE --> " + resultFile);
            Utils.dbgPrint("FULL PATH FROM--> " + copyFromFullPath);
            Utils.dbgPrint("------------------------------");
            Utils.dbgPrint("TO FOLDER --> " + toOutputRootDirectory);
            Utils.dbgPrint("THE FILE --> " + resultFile);
            Utils.dbgPrint("FULL PATH TO--> " + copyToFullPath);

            // - Perform the move from the current temp directory to the root
            // output
            // directory - on PithosFS
            PithosFileSystem.getHadoopPithosConnector()
                    .movePithosObjectToFolder("pithos", // container
                            copyFromFullPath, // source object
                            "", // folder path
                            copyToFullPath); // target object
        }

    }

    /**
     * Keep it as the main class that is defined into jardesc.
     * 
     * @param args
     */
    public static void main(String[] args) {
    }

}
