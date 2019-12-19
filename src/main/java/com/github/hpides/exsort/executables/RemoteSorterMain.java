package com.github.hpides.exsort.executables;

import static com.github.hpides.exsort.FileComparator.assertFileSortedCorrectly;

import com.github.hpides.exsort.RemoteFileSorter;
import com.github.hpides.exsort.RemoteFileSorterClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the executable file for remote file sorting.
 * This will run the RemoteFileSorter to sort a file with a certain memory limit on remote machines.
 * This also asserts that the file was sorted correctly.
 * You can use this to test your solution.
 *
 * If you want to use this manually, make sure to start the RemoteServerMain instances before this.
 * Otherwise, the RemoteFileSorter cannot connect to the servers and will throw an exception.
 *
 * Usage: java -cp build/libs/exsort.jar \
 *          com.github.hpides.exsort.executables.RemoteFileSorter \
 *          inputFile outputFile chunkSize expectedFile remoteAddr:remotePort (1..N times)
 *
 * You should not have to change any code in here.
 */
public final class RemoteSorterMain {
    public static void main(final String[] args) throws IOException {
        if (args.length < 5) {
            System.err.println("Usage: RemoteSorterMain inputFile outputFile chunkSize expectedFile "
                    + "[remoteAddr:remotePort] (1..N times)");
            System.exit(1);
        }
        final String inputFileName = args[0];
        final String outputFileName = args[1];
        final int chunkSize = Integer.parseInt(args[2]);
        final String expectedFileName = args[3];

        final int numFixedArgs = 4;
        final int numRemoteSorters = args.length - numFixedArgs;

        final List<RemoteFileSorterClient> remoteSorters = new ArrayList<>(numRemoteSorters);
        for (int i = numFixedArgs; i < args.length; i ++) {
            final String[] remoteAddrPort = args[i].split(":");
            assert remoteAddrPort.length == 2 : "Bad remote address " + args[i];
            final String remoteAddr = remoteAddrPort[0];
            final int remotePort = Integer.parseInt(remoteAddrPort[1]);
            remoteSorters.add(new RemoteFileSorterClient(remoteAddr, remotePort));
        }

        RemoteFileSorter.sortFile(inputFileName, outputFileName, chunkSize, remoteSorters);
        remoteSorters.forEach(RemoteFileSorterClient::close);

        assertFileSortedCorrectly(expectedFileName, outputFileName);
    }
}
