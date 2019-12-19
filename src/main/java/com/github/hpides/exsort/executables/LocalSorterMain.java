package com.github.hpides.exsort.executables;

import static com.github.hpides.exsort.FileComparator.assertFileSortedCorrectly;

import com.github.hpides.exsort.LocalFileSorter;
import java.io.IOException;

/**
 * This is the executable file for local file sorting.
 * This will run the LocalFileSorter to sort a file with a certain memory limit.
 * This also asserts that the file was sorted correctly.
 * You can use this to test your solution.
 *
 * Usage: java -cp build/libs/exsort.jar \
 *          com.github.hpides.exsort.executables.LocalSorterMain \
 *          inputFile outputFile chunkSize expectedFile
 *
 * You should not have to change any code in here.
 */
public final class LocalSorterMain {
    public static void main(final String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: LocalSorterMain inputFile outputFile chunkSize expectedFile");
            System.exit(1);
        }
        final String inputFileName = args[0];
        final String outputFileName = args[1];
        final int chunkSize = Integer.parseInt(args[2]);
        final String expectedFileName = args[3];

        LocalFileSorter.sortFile(inputFileName, outputFileName, chunkSize);
        assertFileSortedCorrectly(expectedFileName, outputFileName);
    }
}
