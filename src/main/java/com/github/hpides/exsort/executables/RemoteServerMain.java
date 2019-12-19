package com.github.hpides.exsort.executables;

import com.github.hpides.exsort.RemoteFileSorterServer;

/**
 * This is the executable file for the remote file sorting server.
 *
 * Usage: java -cp build/libs/exsort.jar \
 *          com.github.hpides.exsort.executables.RemoteServerMain port
 *
 * You should not have to change any code in here.
 */
public final class RemoteServerMain {
    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: RemoteServerMain port");
            System.exit(1);
        }

        final int port = Integer.parseInt(args[0]);
        new RemoteFileSorterServer(port).run();
    }
}
