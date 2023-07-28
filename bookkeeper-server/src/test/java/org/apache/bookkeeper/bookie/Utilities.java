package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.charset.StandardCharsets.UTF_8;


public class Utilities {

    public static void createFile(File fl, String key, String magic, int version, int headerMKLen) throws FileNotFoundException{
        try (FileChannel myWriter = new RandomAccessFile(fl, "rw").getChannel()) {
                byte[] headerMK = key.getBytes();
                int signature = ByteBuffer.wrap(magic.getBytes(UTF_8)).getInt();
                ByteBuffer headerBB = ByteBuffer.allocate(20+headerMK.length);
                
                headerBB.putInt(signature);
                headerBB.putInt(version);
                headerBB.putInt(headerMKLen);
                headerBB.put(headerMK);
                headerBB.rewind();
                myWriter.position(0);
                myWriter.write(headerBB);
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

}
