package com.tubemogul.stats.logsplitter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import kafka.log.FileMessageSet;
import kafka.message.MessageAndOffset;
import scala.collection.Iterator;

/**
 * Splits log segments affected by https://issues.apache.org/jira/browse/KAFKA-3323
 */
public class LogSplitter {

    public static void main(String [] args) throws IOException {
        File partition = new File(args[0]);
        File[] indexes = partition.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.contains(".index");
            }
        });
        System.out.println("Deleting indexes");
        for (File index : indexes) {
            Files.delete(index.toPath());
        }


        File[] logs = partition.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.contains(".log");
            }
        });
        for (File log : logs) {
            System.out.println("Checking " + log.getPath());
            FileMessageSet fileMessageSet = new FileMessageSet(log, false);
            Iterator<MessageAndOffset> logIter = fileMessageSet.iterator();

            //Holds the offsets that we need to split on
            List<Long> splitOffsets = new ArrayList<Long>();

            long prevSplit = Long.valueOf(log.getName().split("\\.")[0]);
            splitOffsets.add(prevSplit);

            while (logIter.hasNext()) {
                MessageAndOffset mao = logIter.next();
                /*
                    If the current offset is more than an integer above the prevSplit
                    record this one as a split point
                 */
                if ((mao.offset() - prevSplit) > Integer.MAX_VALUE) {
                    prevSplit = mao.offset();
                    splitOffsets.add(mao.offset());
                }
            }

            if (splitOffsets.size() > 1) {
                int prevPos = 0;
                for (int i = 0; i < splitOffsets.size(); i++) {
                    // look at offsets two at a time
                    long offset1 = splitOffsets.get(i);
                    int pos1 = fileMessageSet.searchFor(offset1, prevPos).position();

                    int pos2;
                    if (i+1 < splitOffsets.size()) {
                        long offset2 = splitOffsets.get(i + 1);
                        // get the position of each offset, starting from the previous position
                        pos2 = fileMessageSet.searchFor(offset2, pos1).position();
                    } else {
                        pos2 = fileMessageSet.end();
                    }
                    prevPos = pos2;

                    // create a new file for this log chunk
                    Path file = Files.createFile(new File(partition, String.format("%020d", offset1) + ".tmp").toPath());
                    FileChannel channel = FileChannel.open(
                            file,
                            StandardOpenOption.WRITE
                    );
                    fileMessageSet.writeTo(channel, pos1, pos2-pos1);
                }
                // Delete the log now that we have written out its chunks
                Files.delete(log.toPath());
            }
        }

        // rename all temp logs
        File[] newLogs = partition.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.contains(".tmp");
            }
        });
        for (File log : newLogs) {
            Files.move(
                    log.toPath(),
                    new File(partition, log.getName().replace(".tmp", ".log")).toPath()
            );
        }
    }

}
