package com.github.hpides.exsort;

import com.github.hpides.exsort.LocalFileSorter.KVPair;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * This class is the core of this exercise. You have to write you code in here. Look at the docs for the sort method
 * for details on the exact task.
 */
public final class RemoteFileSorter {

    static class Pair<First, Last> {
        public First first;
        public Last last;

        Pair(First first, Last last) {
            this.first = first;
            this.last = last;
        }
    }

    /**
     * This is the core sorting function for remote files. You should write a method that sorts a file on remote nodes,
     * collects the sorted files from the remote nodes and sorts them in the node running this method. The node running
     * the RemoteFileSorter has a certain memory limit given as `chunkSizeInBytes`. For simplicity, assume that all
     * remote nodes have the same memory limits when requesting a remote sort. Also assume that the input file has the
     * same name on each node.
     *
     * The collecting server on which this method is called does not have a file to sort. It only requests sorted files
     * from other nodes and then stores a globally sorted file.
     *
     * Have a look at the docs of the RemoteFileSorterClient on how to interact with a remote node for sorting. Be aware
     * that some remote calls are blocking and some are not. Also think of the implications this has on parallel
     * execution and think of the memory implications that collecting multiple chunks on one server has with regard to
     * the available memory.
     *
     * @param inputFileName Name of the remote input files to be sorted (same on all nodes)
     * @param outputFileName Name of the local file that should contain the sorted output.
     * @param chunkSizeInBytes Determines how many bytes fit into memory. Usually, this would be close to the size of
     *                         the actual memory, but for this exercise we can limit to less for simplicity.
     * @param remoteFileSorters The list of remote nodes that contain a part of the file that should be sorted.
     *
     * @throws IOException You do not have to deal with exceptions of file handling. If some of the file operations
     *                     fail, just let them escalate. In the tests, we will not require file error handling.
     */
    public static void sortFile(final String inputFileName, final String outputFileName,
            final int chunkSizeInBytes, final List<RemoteFileSorterClient> remoteFileSorters) throws IOException {
        // TODO: You code here
        remoteFileSorters.forEach(sorter -> sorter.sortRemoteFile(inputFileName, outputFileName, chunkSizeInBytes));
        remoteFileSorters.forEach(RemoteFileSorterClient::waitForCommandToComplete);

        remoteFileSorters.forEach(sorter -> sorter.chunkRemoteFile(outputFileName, chunkSizeInBytes));
        remoteFileSorters.forEach(RemoteFileSorterClient::waitForCommandToComplete);

        var fileInputStreams = remoteFileSorters
                .stream()
                .map(sorter -> new Pair<>(sorter, sorter.getNextFileChunk()))
                .filter(pair -> pair.last.isPresent())
                .map(pair -> new Pair<>(pair.first, pair.last.get()))
                .map(pair -> {
                    try{
                        return new Pair<>(pair.first, new FileInputStream(pair.last.getAbsoluteFile()));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());

        var fileInputScanners = fileInputStreams.stream().map(pair -> new Pair<>(pair.first, new Scanner(pair.last))).collect(Collectors.toList());

        var inputHeaders = new PriorityQueue<KVPair<String, Pair<RemoteFileSorterClient, Scanner>>>();
        fileInputScanners.forEach(pair -> {
            if (pair.last.hasNext()) {
                inputHeaders.add(new KVPair<>(pair.last.nextLine(), pair));
            } else {
                var nextChunk = pair.first.getNextFileChunk();
                if (nextChunk.isPresent()) {
                    try {
                        var inputStream = new FileInputStream(nextChunk.get().getAbsoluteFile());
                        fileInputStreams.add(new Pair<>(pair.first, inputStream));
                        var inputScanner = new Scanner(inputStream);
                        fileInputScanners.add(new Pair<>(pair.first, inputScanner));
                        if (inputScanner.hasNext()) {
                            inputHeaders.add(new KVPair<>(inputScanner.nextLine(), new Pair<>(pair.first, inputScanner)));
                        }
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        var output = new FileOutputStream(outputFileName);

        while (!inputHeaders.isEmpty()) {
            var smallest = inputHeaders.poll();
            output.write(smallest.key.getBytes());
            output.write('\n');
            if (smallest.value.last.hasNext()) {
                inputHeaders.add(new KVPair<>(smallest.value.last.nextLine(), smallest.value));
            } else {
                var nextChunk = smallest.value.first.getNextFileChunk();
                if (nextChunk.isPresent()) {
                    try {
                        var inputStream = new FileInputStream(nextChunk.get().getAbsoluteFile());
                        fileInputStreams.add(new Pair<>(smallest.value.first, inputStream));
                        var inputScanner = new Scanner(inputStream);
                        fileInputScanners.add(new Pair<>(smallest.value.first, inputScanner));
                        if (inputScanner.hasNext()) {
                            inputHeaders.add(new KVPair<>(inputScanner.nextLine(), new Pair<>(smallest.value.first, inputScanner)));
                        }
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        fileInputScanners.forEach(pair -> pair.last.close());
        fileInputStreams.forEach(pair -> {
            try {
                pair.last.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        output.close();
    }
}
