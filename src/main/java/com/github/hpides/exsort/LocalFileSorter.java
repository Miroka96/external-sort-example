package com.github.hpides.exsort;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * This class is the core of this exercise. You have to write you code in here. Look at the docs for the two methods
 * for details on the exact task.
 */
public final class LocalFileSorter {

    static class KVPair<Key extends Comparable<Key>, Value> implements Comparable<KVPair<Key, Value>> {
        public Key key;
        public Value value;

        KVPair(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(KVPair<Key, Value> pair) {
            return key.compareTo(pair.key);
        }
    }

    /**
     * This is the core sorting function for a local file. You should write a method that takes a file and sorts it
     * under a certain memory constraint. The `chunkSizeInBytes` determines how many bytes fit into memory.
     *
     * @param inputFileName    Name of the input file to be sorted.
     * @param outputFileName   Name of the file that should contain the sorted output.
     * @param chunkSizeInBytes Determines how many bytes fit into memory. Usually, this would be close to the size of
     *                         the actual memory, but for this exercise we can limit to less for simplicity.
     * @throws IOException You do not have to deal with exceptions of file handling. If some of the file operations
     *                     fail, just let them escalate. In the tests, we will not require file error handling.
     */
    public static void sortFile(final String inputFileName, final String outputFileName,
                                final long chunkSizeInBytes) throws IOException {
        var inputFiles = chunkFile(inputFileName, chunkSizeInBytes);
        List<FileReader> sortedInputFileReaders = inputFiles.stream().map(inputfile -> {
            try {
                var outputFile = File.createTempFile("sorted", ".tmp");
                reallySortFile(inputfile.getAbsolutePath(), outputFile.getAbsolutePath(), chunkSizeInBytes);
                return outputFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).map(File::getAbsoluteFile).map(file -> {
            try {
                return new FileReader(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        var sortedInputs = sortedInputFileReaders.stream().map(Scanner::new).collect(Collectors.toList());
        var inputHeaders = new PriorityQueue<KVPair<String, Scanner>>();
        sortedInputs.forEach(scanner -> {
            if (scanner.hasNext()) {
                inputHeaders.add(new KVPair<>(scanner.nextLine(), scanner));
            }
        });

        var output = new FileOutputStream(outputFileName);

        while (!inputHeaders.isEmpty()) {
            var smallest = inputHeaders.poll();
            output.write(smallest.key.getBytes());
            output.write('\n');
            if (smallest.value.hasNext()) {
                inputHeaders.add(new KVPair<>(smallest.value.nextLine(), smallest.value));
            }
        }

        sortedInputs.forEach(Scanner::close);
        sortedInputFileReaders.forEach(fileReader -> {
            try {
                fileReader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        output.close();
    }

    public static void reallySortFile(final String inputFileName, final String outputFileName, final long chunkSizeInBytes) throws IOException {
        var inputstream = new FileReader(inputFileName);
        var input = new Scanner(inputstream);
        var output = new FileOutputStream(outputFileName);

        ArrayList<String> strings = new ArrayList<>();
        var inputLength = 0;
        while (input.hasNext()) {
            var nextLine = input.nextLine();
            strings.add(nextLine);
            inputLength += nextLine.length() + 1;
        }
        if (inputLength > chunkSizeInBytes) {
            throw new IOException("Input too large");
        }
        input.close();
        inputstream.close();
        strings.sort(String::compareTo);

        for (String s : strings) {
            output.write(s.getBytes());
            output.write('\n');
        }
        output.close();
    }

    /**
     * This method should split a given file into smaller file chunks with a certain memory limit. We need this method
     * to transfer file chunks between nodes and this can also be used for local sorting, depending on the
     * implementation strategy that you chose. This method should not sort any data, but simply split it into chunks.
     * We recommend looking at the `Files.createTempFile()` method to create a temporary file. There are many other
     * options in Java how to do this and we recommend using `/tmp` as the base directory for this if you create the
     * files manually.
     *
     * @param fileName         Name of the file that should be chunked.
     * @param chunkSizeInBytes Determines how many bytes fit into memory. This is the maximum size that a chunk file
     *                         should have. If a new record does not fit into a chunk, create a new chunk. Do not split
     *                         records. Usually, this would be close to the size of the actual memory, but for this
     *                         exercise we can limit to less for simplicity.
     * @return A list of the temporary chunk files.
     * @throws IOException You do not have to deal with exceptions of file handling. If some of the file operations
     *                     fail, just let them escalate. In the tests, we will not require file error handling.
     */
    public static List<File> chunkFile(final String fileName, final long chunkSizeInBytes) throws IOException {
        var inputstream = new FileReader(fileName);
        var input = new Scanner(inputstream);

        ArrayList<File> outputFiles = new ArrayList<>();

        String nextLine = null;
        while (input.hasNext() || nextLine != null) {
            var outputfile = File.createTempFile("unsorted", ".tmp");
            outputFiles.add(outputfile);
            var output = new FileOutputStream(outputfile);
            var outputLength = 0;

            do {
                if (nextLine == null) {
                    nextLine = input.nextLine();
                }
                if (outputLength + nextLine.length() + 1 <= chunkSizeInBytes) {
                    outputLength += nextLine.length() + 1;
                    output.write(nextLine.getBytes());
                    output.write('\n');
                    nextLine = null;
                } else {
                    break;
                }
            } while (input.hasNext());

            output.close();
        }
        input.close();
        inputstream.close();

        return outputFiles;
    }
}
