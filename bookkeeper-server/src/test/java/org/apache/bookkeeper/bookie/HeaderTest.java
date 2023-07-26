package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.net.BookieId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.nio.charset.StandardCharsets.UTF_8;

@RunWith(value=Parameterized.class)
public class HeaderTest{
    private FileInfo fi;
    private String exception;
    private File fl = new File(Variables.LEDGER_FILE_INDEX);
    private String magic;
    private String key;
    private int version;
    private int headerMKLen;
    private boolean del;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exception                  | magic  | key    |version | headerMKLen | del  |
            { "IOException"              , "BKLE" , "abcd" , 1      , 4           , true }, 
            { "IOException"              , "ABCD" , "abcd" , 1      , 4           , false},
            { "IOException"              , "BKLE" , "abcd" , 0      , 4           , false},
            { "IOException"              , "BKLE" , "abcd" , 2      , 4           , false},
            { "IOException"              , "BKLE" , "abcd" , 1      , -1          , false},
            { "BufferUnderflowException" , "BKLE" , "abcd" , 1      , 5           , false},
            { null                       , "BKLE" , "abcd" , 1      , 4           , false},
        });
    }

    public HeaderTest(String exception, String magic, String key, int version, int headerMKLen, boolean del){
        this.exception = exception;
        this.magic = magic;
        this.version = version;
        this.key = key;
        this.headerMKLen = headerMKLen;
        this.del = del;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        fi = new FileInfo(fl, mk, ver);
        if(del){
            fl.delete();
        }else{
            //Popolo il file per i test
            try (
            FileChannel myWriter = new RandomAccessFile(fl, "rw").getChannel()) {
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
    @Test
    public void readHeaderTest(){
        try{
            fi.readHeader();
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}