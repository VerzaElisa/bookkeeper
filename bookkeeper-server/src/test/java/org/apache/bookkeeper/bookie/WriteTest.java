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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.nio.charset.StandardCharsets.UTF_8;

@RunWith(value=Parameterized.class)
public class WriteTest{
    private FileInfo fi;
    private File fl = new File(Variables.LEDGER_FILE_INDEX);
    private String magic = "BKLE";
    private String key;
    private int version;
    private int headerMKLen;
    private boolean del;
    private static final int fileLen = 1000;
    private ByteBuffer bb;
    private ByteBuffer[] writeBbArray;
    private long exp;
    private long writePos;
    private int wBbSize;
    private Integer writeBuffArray;
    private String wExcept;
    private int explicitLacBufLength = 0;
    private FileChannel fc;
    private FileChannel fc_spy;
    private boolean nullWrite;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | writeBuffArray | writePos  | wBbSize        | nullWrite | wExcept                          |
            { 1              , -1025     , -1025-fileLen , false      , "IllegalArgumentException"       }, 
            { null           , 0         , 0             , false      , "NullPointerException"           },    
            { 0              , 0         , 0             , false      , "ArrayIndexOutOfBoundsException" },
            { 1              , 0         , 0             , false      , "ShortWriteException"            }, 
            { 1              , 0         , fileLen       , false      , null                             },
            { 1              , 0         , fileLen+1     , false      , null                             },  
            { 1              , fileLen+1 , 1             , false      , null                             },   
            { 1              , fileLen+1 , 3             , true       , "IOException"                    },           
        }); 
    }

    public WriteTest(Integer writeBuffArray, long writePos, int wBbSize, boolean nullWrite, String wExcept){
        this.writePos = writePos;
        this.wBbSize = wBbSize;
        this.wExcept = wExcept;
        this.writeBuffArray = writeBuffArray;
        this.nullWrite = nullWrite;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchAlgorithmException {
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        fi = new FileInfo(fl, mk, ver);
        //Viene scritto l'header e il contenuto randomico del file
        ByteBuffer lac = Utilities.bbCreator(0L, 100L, Math.abs(explicitLacBufLength)).nioBuffer();
        lac.rewind();
        byte[] lac_byte = new byte[lac.remaining()];
        lac.get(lac_byte);
        lac.rewind();

        Utilities.createFile(fl, Variables.MASTER_KEY, magic, 1, Variables.MASTER_KEY.length(), explicitLacBufLength, 0, lac_byte);
        Utilities.writeOnFile(fileLen, fl);

    }

    @Before
    public void writeSetUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
        if(writeBuffArray != null){
            writeBbArray = new ByteBuffer[(int) writeBuffArray];
            if(writeBuffArray > 0){
                //Viene creato un buffer da inserire nel byte buffer array
                writeBbArray[0] = ByteBuffer.allocate(Math.abs(wBbSize));
                writeBbArray[0].rewind();
                Random random = new Random();
                byte[] fillBuff = new byte[Math.abs(wBbSize)];
                random.nextBytes(fillBuff);
                writeBbArray[0].put(fillBuff);
                writeBbArray[0].rewind();
            }
        } 
        //Spy del file channel per far ritornare la write zero
        if(nullWrite){
            fc = new RandomAccessFile(fl, "rw").getChannel();
            fc_spy = spy(fc);
            Mockito.when(fc_spy.write(writeBbArray)).thenReturn(0L);
            Utilities.setPrivate(fi, fc_spy, "fc");
            writeBbArray[0].rewind();
        }

    }

    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
    }

    @Test
    public void writeTest(){
         try{
            long ret = fi.write(writeBbArray, writePos);
            Assert.assertEquals(wBbSize, ret);
            Assert.assertEquals(Math.max(fileLen+1024, writePos+wBbSize+1024), fl.length());
        }catch(Exception e){    
            Assert.assertEquals(wExcept, e.getClass().getSimpleName());
        }
    }
}