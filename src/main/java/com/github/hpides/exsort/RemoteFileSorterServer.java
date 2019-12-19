package com.github.hpides.exsort;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This server runs on a "remote" server and receives requests to sort files.
 * It then locally executes the sorting and chunking operations and provides the client with the individual chunks.
 *
 * You should not have to change any code in here.
 */
public class RemoteFileSorterServer implements Runnable {

    public static final String SORT_CMD = "SORT";
    public static final String COMMAND_COMPLETE_CMD = "SORT_COMPLETE";
    public static final String CHUNK_CMD = "CHUNK";
    public static final String GET_CHUNK_CMD = "GET_CHUNK";
    public static final String SHUTDOWN_CMD = "SHUTDOWN";

    private final int port;
    private List<File> chunkFiles;
    private int currentChunkFile;

    public RemoteFileSorterServer(final int port) {
        this.port = port;

        this.chunkFiles = new ArrayList<>();
        this.currentChunkFile = 0;
    }

    private void receiveCommands(final DataOutputStream responseSender, final DataInput commandReceiver)
            throws IOException {
        while (true) {
            final String cmd;
            try {
                cmd = commandReceiver.readUTF();
            } catch (final IOException e) {
                throw new RuntimeException("Error receiving command from remote file sorter!", e);
            }

            final String[] cmdParts = cmd.split(",");
            if (cmdParts.length < 1) {
                throw new RuntimeException("Error receiving command from remote file sorter!");
            }

            final String operation = cmdParts[0];
            switch (operation) {
                case SORT_CMD: {
                    assert cmdParts.length == 4 : SORT_CMD + " requires 3 args, got " + (cmdParts.length - 1);
                    final String inputFileName = cmdParts[1];
                    final String outputFileName = cmdParts[2];
                    final int chunkSizeInBytes = Integer.parseInt(cmdParts[3]);
                    this.sortFile(inputFileName, outputFileName, chunkSizeInBytes);
                    break;
                }
                case COMMAND_COMPLETE_CMD: {
                    this.ackSortComplete(responseSender);
                    break;
                }
                case CHUNK_CMD: {
                    assert cmdParts.length == 3 : CHUNK_CMD + " requires 2 args, got " + (cmdParts.length - 1);
                    final String inputFileName = cmdParts[1];
                    final int chunkSizeInBytes = Integer.parseInt(cmdParts[2]);
                    this.chunkFile(inputFileName, chunkSizeInBytes);
                    break;
                }
                case GET_CHUNK_CMD: {
                    this.getAndSendFileChunk(responseSender);
                    break;
                }
                case SHUTDOWN_CMD: {
                    return;
                }
                default: {
                    throw new RuntimeException("Unknown command from remote file sorter! " + operation);
                }
            }
        }
    }

    @Override
    public void run() {
        try (final ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(this.port));
            final Socket socket = serverSocket.accept();
            final DataOutputStream responseSender = new DataOutputStream(socket.getOutputStream());
            final DataInputStream commandReceiver = new DataInputStream(socket.getInputStream());

            // Run until shutdown command
            this.receiveCommands(responseSender, commandReceiver);

            responseSender.close();
            commandReceiver.close();
        } catch (final IOException e) {
            throw new RuntimeException("Cannot open socket on remote file sorter client!", e);
        }
    }

    private void sortFile(final String inputFileName, final String outputFileName, final int chunkSizeInBytes)
            throws IOException {
        LocalFileSorter.sortFile(inputFileName, outputFileName, chunkSizeInBytes);
    }

    private void chunkFile(final String fileName, final int chunkSizeInBytes) throws IOException {
        this.chunkFiles = LocalFileSorter.chunkFile(fileName, chunkSizeInBytes);
        this.currentChunkFile = 0;
    }

    private Optional<File> getFileChunk() {
        if (this.currentChunkFile >= this.chunkFiles.size()) {
            return Optional.empty();
        }

        return Optional.of(this.chunkFiles.get(this.currentChunkFile++));
    }

    private void getAndSendFileChunk(final DataOutputStream responseSender) {
        final Optional<File> chunkFileOpt = this.getFileChunk();

        try {
            if (chunkFileOpt.isEmpty()) {
                responseSender.writeInt(-1);
                return;
            }

            final File chunkFile = chunkFileOpt.get();
            final int fileLength = (int) chunkFile.length();
            responseSender.writeInt(fileLength);
            Files.copy(chunkFile.toPath(), responseSender);
        } catch (final IOException e) {
            throw new RuntimeException("Error sending file chunk to remote file sorter!", e);
        }
    }

    private void ackSortComplete(final DataOutputStream responseSender) {
        try {
            responseSender.writeBoolean(true);
        } catch (final IOException e) {
            throw new RuntimeException("Error sending sort completion to remote file sorter!", e);
        }
    }
}
