package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(value=Parameterized.class)
public class ReadTest{
    private FileInfo fi;
    private String exception;
    private File fl = new File(Variables.TEST_FOLDER+"/"+Variables.LEDGER_FILE_INDEX);
    private String magic = "BKLE";
    private static final int fileLen = 1000;
    private int start;
    private int toSum;
    private boolean bestEffort;
    private ByteBuffer bb;

    private long exp;
    private int explicitLacBufLength = 0;
    private  boolean fileExists;
    private FileChannel fc;
    private FileChannel fc_spy;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exception                  | start     | toSum   | bestEffort | fileExists |
            { "IllegalArgumentException" , -1025     , -1      , true       , true       }, 
            { null                       , fileLen+1 , 0       , true       , true       },    
            { null                       , -1024     , 0       , true       , true       },
            { "ShortReadException"       , -1024     , 0       , false      , true       }, 
            { null                       , -1024     , -1      , true       , true       },
            { null                       , fileLen   , +1      , true       , true       },  
            { "ShortReadException"       , fileLen   , +1      , false      , true       },   
            { null                       , fileLen+1 , -1      , true       , false      },    
            { null                       , -1024     , -2024   , true       , true       },

            {null                        , 0         , 1025    , true       , true       }
        }); 
    }

    public ReadTest(String exception, int start, int toSum, boolean bestEffort, boolean fileExists){
        this.exception = exception;
        this.start = start;
        this.toSum = toSum;
        this.bestEffort = bestEffort;
        this.fileExists = fileExists;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchAlgorithmException {
        Utilities.createDirectory(Variables.TEST_FOLDER);
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        fi = new FileInfo(fl, mk, ver);
        //Viene reso accessibile il file channel
        fc = new RandomAccessFile(fl, "rw").getChannel();
        fc_spy = spy(fc);
        Utilities.setPrivate(fi, fc_spy, "fc");

        //Viene scritto l'header e il contenuto randomico del file
        if(fileExists){
            ByteBuffer lac = Utilities.bbCreator(0L, 100L, Math.abs(explicitLacBufLength)).nioBuffer();
            lac.rewind();
            byte[] lac_byte = new byte[lac.remaining()];
            lac.get(lac_byte);
            lac.rewind();

            Utilities.createFile(fl, Variables.MASTER_KEY, magic, 1, Variables.MASTER_KEY.length(), explicitLacBufLength, 0, lac_byte);
            Utilities.writeOnFile(fileLen, fl);
        }
        //Viene creato il bytebuffer di input della giusta dimensione
        bb = ByteBuffer.allocate((Math.abs(start-fileLen)+toSum));
        bb.rewind();
        //Viene assegnato il valore atteso in caso di successo
        exp = Math.min(Math.abs(start-fileLen), bb.capacity());
        if(!fileExists || start>fileLen){
            exp = 0;
        }
    }

    @After
    public void onClose(){
        File parent = new File(Variables.TEST_FOLDER); 
        Utilities.deleteDirectory(parent);
    }
    @Test
    public void readTest(){
        try{
            long ret = fi.read(bb, start, bestEffort);
            if(toSum == -2024){
                verify(fc_spy, times(0)).read(any(ByteBuffer.class), anyLong());
            }
            Assert.assertEquals(exp, ret);
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}