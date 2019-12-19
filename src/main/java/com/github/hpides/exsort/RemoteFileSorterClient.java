package com.github.hpides.exsort;

import static com.github.hpides.exsort.RemoteFileSorterServer.CHUNK_CMD;
import static com.github.hpides.exsort.RemoteFileSorterServer.COMMAND_COMPLETE_CMD;
import static com.github.hpides.exsort.RemoteFileSorterServer.GET_CHUNK_CMD;
import static com.github.hpides.exsort.RemoteFileSorterServer.SHUTDOWN_CMD;
import static com.github.hpides.exsort.RemoteFileSorterServer.SORT_CMD;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Optional;

/**
 * This is the client that runs on the collecting node and connects to one RemoteFileSorterServer.
 * It can be used to
 *   - request to sort a file,
 *   - request to chunk a file,
 *   - request next chunk of previously chunked file.
 *
 * There is always a 1-to-1 connection between a client and a server.
 *
 * Remote sorting (`sortRemoteFile()`) and chunking (`chunkRemoteFile()`) requests are asynchronous.
 * That means that you need to call the blocking `waitForCommandToComplete()` after them to know when they are complete.
 *
 * You do not need to take care of creating these clients, you just need to use them correctly.
 * Read the docs of the individual methods for more details.
 *
 * You should not have to change any code in here.
 */
public class RemoteFileSorterClient {
    private static final int RESPONSE_TIMEOUT = 15 * 1000;  // 15 seconds

    private final String remoteHostIp;
    private final int remoteHostPort;

    private final Socket socket;
    private final DataOutputStream requestStream;
    private final DataInputStream responseStream;
    private final String clientSuffix;

    private boolean isOpen;

    /**
     * Creates a new RemoteFileSorterClient that immediately connects to a RemoteFileSortingServer.
     * If the server is not available, this will most likely throw an exception.
     * Make sure that the server is running beforehand.
     * You do not need to take care of creating these clients, you just need to use them correctly.
     */
    public RemoteFileSorterClient(final String remoteHostIp, final int remoteHostPort) {
        this.remoteHostIp = remoteHostIp;
        this.remoteHostPort = remoteHostPort;
        this.clientSuffix = remoteHostIp.replace(".", "-") + "-" + remoteHostPort;

        try {
            this.socket = new Socket(remoteHostIp, remoteHostPort);
            this.socket.setSoTimeout(RESPONSE_TIMEOUT);
            this.requestStream = new DataOutputStream(this.socket.getOutputStream());
            this.responseStream = new DataInputStream(this.socket.getInputStream());
        } catch (final IOException e) {
            throw new RuntimeException("Cannot connect to remote file sorter client!", e);
        }
        this.isOpen = true;
    }

    /**
     * Tells the RemoteFileSorterServer to sort a given file under a given memory constraint.
     * After the remote sorting is complete, the output file on the remote node will be the sorted version of the
     * input file.
     *
     * Usually, a node would not tell a remote server how to sort the data as it does not know the memory constraints
     * of that server. In this exercise, we assume all nodes have the same memory constraints for simplicity.
     * You can simply pass on the `chunkSizeInBytes` value from the RemoteFileSorter.
     *
     * @param inputFileName Name of the input file to be sorted on the remote node.
     * @param outputFileName Name of the sorted output file on the remote node. This can be the same output file as
     *                       given to the RemoteFileSorter.
     * @param chunkSizeInBytes Determines how many bytes fit into memory on the remote node. Usually, this would be
     *                         close to the size of the actual memory, but for this exercise we can limit to less for
     *                         simplicity. This can be the same value as in the RemoteFileSorter.
     */
    public void sortRemoteFile(final String inputFileName, final String outputFileName, final int chunkSizeInBytes) {
        assert this.isOpen : "RemoteFileSorterClient was closed!";
        final String cmd = String.format("%s,%s,%s,%d", SORT_CMD, inputFileName,
                outputFileName + this.clientSuffix, chunkSizeInBytes);
        try {
            this.requestStream.writeUTF(cmd);
        } catch (final IOException e) {
            throw new RuntimeException("Cannot send sort command to remote file sorter client!", e);
        }
    }

    /**
     * This requests to split a remote file into smaller file chunks as specified by the `chunkSizeInBytes`.
     * The chunks are located on the remote server and can be accessed sequentially by calling `getNextFileChunk()`.
     * @param fileName The name of the remote file to chunk. This should usually be the outputFileName of the sorting call.
     * @param chunkSizeInBytes Number of bytes that should be present in one chunk file. Remember here that the
     *                         limit can be different from the previous sorting limit, as the RemoteFileSorter
     *                         needs to collect N chunks from N remote nodes and all N chunks need to fit into
     *                         memory on the collecting node.
     */
    public void chunkRemoteFile(final String fileName, final int chunkSizeInBytes) {
        assert this.isOpen : "RemoteFileSorterClient was closed!";
        final String cmd = String.format("%s,%s,%d", CHUNK_CMD, fileName + this.clientSuffix, chunkSizeInBytes);
        try {
            this.requestStream.writeUTF(cmd);
        } catch (final IOException e) {
            throw new RuntimeException("Cannot send chunk command to remote file sorter client!", e);
        }
    }

    /**
     * This call retrieves the next file chunk from the remote server. It is a blocking call.
     * To keep the chunking logic simple, this method can only be called after a `chunkRemoteFile()` call. A new
     * chunking call will overwrite the previous one and the old state will be lost. For this exercise, you will most
     * likely only need to call this once and then retrieve the resulting chunks. This method takes care of all
     * networking logic for file transfers. You do not need to deal with the details of that for this exercise.
     *
     * @return If there are no chunks left, this will return an empty optional. Otherwise, it will return an optional of
     *         the chunk, now located on the collecting server after the network transfer.
     */
    public Optional<File> getNextFileChunk() {
        assert this.isOpen : "RemoteFileSorterClient was closed!";
        try {
            this.requestStream.writeUTF(GET_CHUNK_CMD);
            final int numBytesToRead = this.responseStream.readInt();

            // No more chunks for this file
            if (numBytesToRead == -1) {
                return Optional.empty();
            }

            final File tempChunkFile;
            try {
                tempChunkFile = File.createTempFile("temp-chunk-file-", ".txt");
            } catch (final IOException e) {
                throw new RuntimeException("Cannot create temp file on server!", e);
            }

            final byte[] chunkData =  this.responseStream.readNBytes(numBytesToRead);
            Files.write(tempChunkFile.toPath(), chunkData);
            return Optional.of(tempChunkFile);
        } catch (final IOException e) {
            throw new RuntimeException("Cannot get next chunk from remote file sorter client!", e);
        }
    }

    /**
     * This method is used to wait for te remote operation to complete. As sorting and chunking is asynchronous, you
     * need to call this method afterwards to know when it is complete. Unlike sorting and chunking, this call is
     * blocking! It will return after the remote operation is finished.
     */
    public void waitForCommandToComplete() {
        assert this.isOpen : "RemoteFileSorterClient was closed!";
        try {
            this.requestStream.writeUTF(COMMAND_COMPLETE_CMD);

            // The response is always `true`, no need to check.
            this.responseStream.readBoolean();
        } catch (final IOException e) {
            throw new RuntimeException("Cannot acknowledge completion from remote file sorter client!", e);
        }
    }

    /**
     * This method closes the remote server. If you do not call this method, the RemoteFileSorterServer will run
     * indefinitely. Unless you write custom test code or executables, you will not need to deal with this method.
     */
    public void close() {
        if (!this.isOpen) {
            return;
        }
        this.isOpen = false;
        try {
            this.requestStream.writeUTF(SHUTDOWN_CMD);
            Thread.sleep(1000);

            this.responseStream.close();
            this.requestStream.close();
            this.socket.close();
        } catch (final IOException | InterruptedException e) {
            System.out.println("Error while closing socket and shutting down. Ignoring.");
        }
    }

    @Override
    public String toString() {
        return "RemoteFileSorterClient{" +
                "remoteHostIp=" + this.remoteHostIp +
                ", remoteHostPort=" + this.remoteHostPort +
                '}';
    }
}
