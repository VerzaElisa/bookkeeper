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

import org.apache.commons.lang3.RandomUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


public class Utilities {
    
    public static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);        
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }

    public static void setPrivate(Object classToModify, Object newValue, String fieldName) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
        Field privateField = classToModify.getClass().getDeclaredField(fieldName);
        privateField.setAccessible(true);
        privateField.set(classToModify, newValue);  
    }

    public static void createFile(File fl, String key, String magic, int version, int headerMKLen, int buffLen) throws FileNotFoundException{
        try (FileChannel myWriter = new RandomAccessFile(fl, "rw").getChannel()) {
                byte[] headerMK = key.getBytes();
                int signature = ByteBuffer.wrap(magic.getBytes(UTF_8)).getInt();
                ByteBuffer headerBB = ByteBuffer.allocate(buffLen);
                
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
