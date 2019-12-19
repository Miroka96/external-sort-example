package com.github.hpides.exsort;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * This class is the core of this exercise. You have to write you code in here. Look at the docs for the two methods
 * for details on the exact task.
 */
public final class LocalFileSorter {

    /**
     * This is the core sorting function for a local file. You should write a method that takes a file and sorts it
     * under a certain memory constraint. The `chunkSizeInBytes` determines how many bytes fit into memory.
     *
     * @param inputFileName Name of the input file to be sorted.
     * @param outputFileName Name of the file that should contain the sorted output.
     * @param chunkSizeInBytes Determines how many bytes fit into memory. Usually, this would be close to the size of
     *                         the actual memory, but for this exercise we can limit to less for simplicity.
     *
     * @throws IOException You do not have to deal with exceptions of file handling. If some of the file operations
     *                     fail, just let them escalate. In the tests, we will not require file error handling.
     */
    public static void sortFile(final String inputFileName, final String outputFileName,
            final long chunkSizeInBytes) throws IOException {
        // TODO: You code here
    }

    /**
     * This method should split a given file into smaller file chunks with a certain memory limit. We need this method
     * to transfer file chunks between nodes and this can also be used for local sorting, depending on the
     * implementation strategy that you chose. This method should not sort any data, but simply split it into chunks.
     * We recommend looking at the `Files.createTempFile()` method to create a temporary file. There are many other
     * options in Java how to do this and we recommend using `/tmp` as the base directory for this if you create the
     * files manually.
     *
     * @param fileName Name of the file that should be chunked.
     * @param chunkSizeInBytes Determines how many bytes fit into memory. This is the maximum size that a chunk file
     *                         should have. If a new record does not fit into a chunk, create a new chunk. Do not split
     *                         records. Usually, this would be close to the size of the actual memory, but for this
     *                         exercise we can limit to less for simplicity.
     *
     * @return A list of the temporary chunk files.
     *
     * @throws IOException You do not have to deal with exceptions of file handling. If some of the file operations
     *                     fail, just let them escalate. In the tests, we will not require file error handling.
     */
    public static List<File> chunkFile(final String fileName, final long chunkSizeInBytes) throws IOException {
        // TODO: You code here
        return null;
    }
}
