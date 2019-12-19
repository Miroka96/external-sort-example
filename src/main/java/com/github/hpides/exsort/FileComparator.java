package com.github.hpides.exsort;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.Scanner;

/**
 * Simply helper class to compare the content of two files for equality.
 * You should not have to change any code in here.
 */
public final class FileComparator {
    /**
     * Checks if two files are identical in content.
     * @return true if files are equal, false otherwise
     */
    public static boolean areEqual(final File expectedFile, final File actualFile) {
        if (Files.notExists(expectedFile.toPath())) {
            System.out.println("File " + expectedFile + " does not exist");
            return false;
        }
        if (Files.notExists(actualFile.toPath())) {
            System.out.println("File " + actualFile + " does not exist");
            return false;
        }

        try (final Scanner expectedScanner = new Scanner(expectedFile); final Scanner actualScanner = new Scanner(actualFile)) {
            while (expectedScanner.hasNextLine()) {
                final String expectedLine = expectedScanner.nextLine();
                if (expectedLine.isBlank()) {
                    break;
                }

                if (!actualScanner.hasNextLine()) {
                    System.out.println("Actual file is too short!");
                    return false;
                }
                final String actualLine = actualScanner.nextLine();
                if (!expectedLine.equals(actualLine)) {
                    System.out.println("File content is not equal!");
                    return false;
                }
            }

            if (actualScanner.hasNextLine()) {
                final String eofLine = actualScanner.nextLine();
                if (!eofLine.isBlank() || actualScanner.hasNextLine()) {
                    System.out.println("Actual file is too long!");
                    return false;
                }
            }

            return true;
        } catch (final FileNotFoundException e) {
            throw new RuntimeException("File deleted after check or maybe permission denied.", e);
        }
    }

    public static void assertFileSortedCorrectly(final String expectedFileName, final String outputFileName) {
        final boolean filesAreEqual = areEqual(new File(expectedFileName), new File(outputFileName));
        if (filesAreEqual) {
            System.out.println("File was sorted correctly.");
            System.exit(0);
        }

        System.exit(1);
    }
}
