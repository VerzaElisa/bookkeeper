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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.nio.charset.StandardCharsets.UTF_8;

@RunWith(value=Parameterized.class)
public class ReadWriteMoveTest{
    private FileInfo fi;
    private String path = "testsFiles/moveFile";
    private String exception;
    private File fl = new File(Variables.LEDGER_FILE_INDEX);
    private File moveFile = new File(path);
    private String magic = "BKLE";
    private String key;
    private int version;
    private int headerMKLen;
    private boolean del;
    private static final int fileLen = 1000;
    private int start;
    private int toSum;
    private boolean bestEffort;
    private ByteBuffer bb;
    private ByteBuffer[] writeBbArray;
    private long exp;
    private long writePos;
    private int wBbSize;
    private Integer writeBuffArray;
    private String wExcept;
    private boolean exists;
    private long size;
    private String mException;
    private int explicitLacBufLength = 0;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exception                  | start     | bbSize  | bestEffort | writeBuffArray | writePos | wBbSize       | wExcept                          | exists | size      | mException |
            { "IllegalArgumentException" , -1025     , -1      , true       , 1              , -1025    , -1025-fileLen , "IllegalArgumentException"       , false  , 100       , null       }, 
            { null                       , fileLen+1 , -1      , true       , null           , 0        , 0             , "NullPointerException"           , true   , 0         , null       },    
            { null                       , -1024     , 0       , true       , 0              , 0        , 0             , "ArrayIndexOutOfBoundsException" , true   , 1         , null       },
            { "ShortReadException"       , -1024     , 0       , false      , 1              , 0         , 0             , "ShortWriteException"           , true  , fileLen+1 , null        }, 
            { null                       , -1024     , -1      , true       , 1              , 0         , fileLen       , null                            , true  , fileLen+1 , null        },
            { null                       , fileLen   , +1      , true       , 1              , 0         , fileLen+1     , null                            , true  , fileLen+1 , null         },  
            { "ShortReadException"       , fileLen   , +1      , false      , 1              , fileLen+1 , 1             , null                            , true  , fileLen+1 , null         },   
        });
    }

    public ReadWriteMoveTest(String exception, int start, int toSum, boolean bestEffort, Integer writeBuffArray, long writePos, int wBbSize, String wExcept, boolean exists, long size, String mException){
        this.exception = exception;
        this.start = start;
        this.toSum = toSum;
        this.bestEffort = bestEffort;
        this.writePos = writePos;
        this.wBbSize = wBbSize;
        this.wExcept = wExcept;
        this.writeBuffArray = writeBuffArray;
        this.exists = exists;
        this.size = size;
        this.mException = mException;
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
        //Viene creato il bytebuffer di input della giusta dimensione
        bb = ByteBuffer.allocate((Math.abs(start-fileLen)+toSum));
        bb.rewind();
        //Viene assegnato il valore atteso in caso di successo
        exp = Math.min(Math.abs(start-fileLen), bb.capacity());
        
    }

    @Before
    public void writeSetUp(){
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
    }

    @Before
    public void moveSetUp() throws IOException{
        if(exists){
            moveFile.createNewFile();
        }
    }

    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
        File myObj1 = new File(path); 
        myObj1.delete();
    }
    @Test
    public void readHeaderTest(){
        try{
            long ret = fi.read(bb, start, bestEffort);
            Assert.assertEquals(exp, ret);
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
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

    @Test
    public void moveTest(){
        try{
            fi.moveToNewLocation(moveFile, size);
            assertTrue(moveFile.exists());
            Assert.assertEquals(Math.min(size, 1024+fileLen), moveFile.length());
        }catch(Exception e){ 
            e.printStackTrace();   
            Assert.assertEquals(mException, e.getClass().getSimpleName());
        }
    }
}