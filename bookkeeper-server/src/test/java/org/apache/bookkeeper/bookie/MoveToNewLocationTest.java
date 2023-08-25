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
public class MoveToNewLocationTest{
    private FileInfo fi;
    private String path = "testsFiles/moveFile";
    private File fl = new File(Variables.LEDGER_FILE_INDEX);
    private File moveFile = new File(path);
    private String magic = "BKLE";
    private String key;
    private int version;
    private int headerMKLen;
    private boolean del;
    private static final int fileLen = 1000;
    private ByteBuffer bb;
    private ByteBuffer[] writeBbArray;
    private long exp;
    private boolean exists;
    private long size;
    private String mException;
    private int explicitLacBufLength = 0;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exists | size      | mException  |
            { false  , 100       , null        }, 
            { true   , 0         , null        },    
            { true   , 1         , null        },
            { true   , fileLen+1 , null        }, 
            { true   , fileLen+1 , null        },
            { true   , fileLen+1 , null        },  
            { true   , fileLen+1 , null        },           
        }); 
    }

    public MoveToNewLocationTest(boolean exists, long size, String mException){
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