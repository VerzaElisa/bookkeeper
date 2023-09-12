package org.apache.bookkeeper.bookie;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.spy;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.mockito.Mockito;


@RunWith(value=Parameterized.class)
public class WriteTest{
    private FileInfo fi;
    private File fl = new File(Variables.TEST_FOLDER+"/"+Variables.LEDGER_FILE_INDEX);
    private String magic = "BKLE";
    private static final int fileLen = 1000;
    private ByteBuffer[] writeBbArray;
    private long writePos;
    private int wBbSize;
    private Integer writeBuffArray;
    private String wExcept;
    private int explicitLacBufLength = 0;
    private FileChannel fc;
    private FileChannel fc_spy;
    private boolean nullWrite;
    private Field sizeField;
    private boolean fcExists;
    private long size;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | writeBuffArray | writePos  | wBbSize        | nullWrite | wExcept                          | fcExists | size |
            { 1              , -1025     , -1025-fileLen , false      , "IllegalArgumentException"       , false    , 2024}, 
            { null           , 0         , 0             , false      , "NullPointerException"           , true     , 1024},    
            { 0              , 0         , 0             , false      , "ArrayIndexOutOfBoundsException" , true     , 1024},
            { 1              , 0         , 0             , false      , "ShortWriteException"            , true     , 1024}, 
            { 1              , 0         , fileLen       , false      , null                             , true     , 2024},
            { 1              , 0         , fileLen+1     , false      , null                             , true     , 2025},  
            { 1              , fileLen+1 , 1             , false      , null                             , true     , 2026},   
            { 1              , fileLen+1 , 3             , true       , "IOException"                    , true     , 2025},           
        }); 
    }

    public WriteTest(Integer writeBuffArray, long writePos, int wBbSize, boolean nullWrite, String wExcept, boolean fcExists, long size){
        this.writePos = writePos;
        this.wBbSize = wBbSize;
        this.wExcept = wExcept;
        this.writeBuffArray = writeBuffArray;
        this.nullWrite = nullWrite;
        this.fcExists = fcExists;
        this.size = size;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchAlgorithmException {
        Utilities.createDirectory(Variables.TEST_FOLDER);
        Configurator.setLevel("org.apache.bookkeeper.bookie.FileInfo", Level.TRACE);
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
        //Viene resa accessibile la variabile size per i test
        sizeField = fi.getClass().getDeclaredField("size");
        sizeField.setAccessible(true);
        if(fcExists){
            //Spy del file channel per far ritornare la write zero
            fc = new RandomAccessFile(fl, "rw").getChannel();
            fc_spy = spy(fc);
            Utilities.setPrivate(fi, fc_spy, "fc");


        }
        if(nullWrite){
            Mockito.when(fc_spy.write(writeBbArray)).thenReturn(0L);
            writeBbArray[0].rewind();
            Utilities.setPrivate(fi, fc_spy, "fc");
        }


    }

    @After
    public void onClose(){
        File parent = new File(Variables.TEST_FOLDER); 
        Utilities.deleteDirectory(parent);
    }

    @Test
    public void writeTest() throws IllegalArgumentException, IllegalAccessException{
         try{
            long ret = fi.write(writeBbArray, writePos);
            Assert.assertEquals(wBbSize, ret);
            Assert.assertEquals(Math.max(fileLen+1024, writePos+wBbSize+1024), fl.length());
            Mockito.verify(fc_spy).force(true);
            Assert.assertEquals(size, sizeField.get(fi));
        }catch(Exception e){    
            Assert.assertEquals(size, sizeField.get(fi));
            Assert.assertEquals(wExcept, e.getClass().getSimpleName());
        }
    }
}