package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.nio.charset.StandardCharsets.UTF_8;


public class Utilities {
    
    public static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);        
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }

    public static Field setPrivate(Object classToModify, Object newValue, String fieldName) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
        Field privateField = classToModify.getClass().getDeclaredField(fieldName);
        privateField.setAccessible(true);
        privateField.set(classToModify, newValue);  
        return privateField;
    }

    public static ByteBuf bbCreator(Long leftLimit, Long rightLimit, int byteBuffLen){
        int i = 0;
        ByteBuffer bb;
        ByteBuf retLac;
        long entry;
        List<Long> entryList = new ArrayList<>();
        bb = ByteBuffer.allocate(byteBuffLen);
        while(i<(byteBuffLen/8)){
            entry = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
            entryList.add(entry);
            
            bb.putLong(entry);
            i++;
        }
        bb.rewind();
        //Conversione ByteBuffer in ByteBuf
        retLac = Unpooled.buffer(bb.capacity());
        bb.rewind();
        retLac.writeBytes(bb);
        bb.rewind();
        return retLac;
    }

    public static void createFile(File fl, String key, String magic, int version, int headerMKLen, int explicitLacBufLength, int stateBits, byte[] lac_byte) throws FileNotFoundException{
        try (FileChannel myWriter = new RandomAccessFile(fl, "rw").getChannel()) {
                byte[] headerMK = key.getBytes();
                int signature = ByteBuffer.wrap(magic.getBytes(UTF_8)).getInt();
                ByteBuffer headerBB = ByteBuffer.allocate(20+headerMK.length+Math.abs(explicitLacBufLength));
                headerBB.putInt(signature);
                headerBB.putInt(version);
                headerBB.putInt(headerMKLen);
                headerBB.put(headerMK);
                headerBB.putInt(stateBits);
                headerBB.putInt(explicitLacBufLength);
                headerBB.put(lac_byte);
                headerBB.rewind();
                myWriter.position(0);
                myWriter.write(headerBB);
                headerBB.rewind();
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
    public static void writeOnFile(int len, File fl) throws NoSuchAlgorithmException, FileNotFoundException{
        ByteBuffer fileContent = ByteBuffer.allocate(len);
        byte[] b = RandomUtils.nextBytes(len);
        fileContent.put(b);
        fileContent.rewind();
        try (FileChannel myWriter = new RandomAccessFile(fl, "rw").getChannel()) {
                myWriter.position(1024);
                myWriter.write(fileContent);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
