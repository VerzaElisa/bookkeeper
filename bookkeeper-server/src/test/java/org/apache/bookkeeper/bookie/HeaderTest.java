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
import org.slf4j.LoggerFactory;

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
    private boolean fileChannel;
    private FileChannel fc;
    private FileChannel fc_spy;
    private int explicitLacBufLength;
    private Field explicitLacField;
    private ByteBuffer ret = null;
    private int full;




    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exception                  | magic  | key    |version | headerMKLen | del   | explicitLacBufLength| fileChannel | full |
            { "IOException"              , "BKLE" , "abcd" , 1      , 4           , true  , 0                   , false       , 0    }, 
            { "IOException"              , "ABCD" , "abcd" , 1      , 4           , false , 0                   , false       , 0    },
            { "IOException"              , "BKLE" , "abcd" , 0      , 4           , false , 0                   , false       , 0    },
            { "IOException"              , "BKLE" , "abcd" , 2      , 4           , false , 0                   , false       , 0    },
            { "IOException"              , "BKLE" , "abcd" , 1      , -1          , false , 0                   , false       , 0    },
            { "BufferUnderflowException" , "BKLE" , "abcd" , 1      , 13          , false , 0                   , false       , 0    },
            { null                       , "BKLE" , "abcd" , 1      , 4           , false , 16                  , false       , 0    },
            { null                       , "BKLE" , "abcd" , 1      , 4           , false , 16                  , false       , 16   },
            { "BufferOverflowException"  , "BKLE" , "abcd" , 1      , 4           , false , 16                  , false       , 15   },
            { "IOException"              , "BKLE" , "abcd" , 1      , 4           , false , 15                  , false       , 0    },
            { "IOException"              , "BKLE" , "abcd" , 1      , 4           , false , 1                   , false       , 0    },
            { "IOException"              , "BKLE" , "abcd" , 1      , 4           , false , -1                  , false       , 0    },
            { null                       , "BKLE" , "abcd" , 1      , 4           , false , 0                   , true        , 0    },

        });
    }

    public HeaderTest(String exception, String magic, String key, int version, int headerMKLen, boolean del, int explicitLacBufLength, boolean fileChannel, int full){
        this.exception = exception;
        this.magic = magic;
        this.version = version;
        this.key = key;
        this.headerMKLen = headerMKLen;
        this.del = del;
        this.fileChannel = fileChannel;
        this.explicitLacBufLength = explicitLacBufLength;
        this.full = full;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        fi = new FileInfo(fl, mk, ver);

        //Viene reso accessibile il campo explicitLac
        explicitLacField = fi.getClass().getDeclaredField("explicitLac");
        explicitLacField.setAccessible(true);

        ByteBuffer lac = Utilities.bbCreator(0L, 100L, Math.abs(explicitLacBufLength)).nioBuffer();
        lac.rewind();
        byte[] lac_byte = new byte[lac.remaining()];
        lac.get(lac_byte);
        lac.rewind();

        if(del){
            fl.delete();
        }else{
            //Popolo il file per i test
            Utilities.createFile(fl, key, magic, version, headerMKLen, explicitLacBufLength, 0, lac_byte);
        }
        //Viene effettuata la spy del FileChannel
        if(fileChannel){
            fc = new RandomAccessFile(fl, "rw").getChannel();
            fc_spy = spy(fc);
            Utilities.setPrivate(fi, fc_spy, "fc");
        }

        if(explicitLacBufLength!=0){
            ret = lac;
        }
        if(full!=0){
            explicitLacField.set(fi, Utilities.bbCreator(200L, 300L, full).nioBuffer());  

        }
    }
    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
    }
    @Test
    public void readHeaderTest(){
        try{
            fi.readHeader();
            if(fileChannel){
                verify(fc_spy, times(0)).size();
            }
            Assert.assertEquals(ret, explicitLacField.get(fi));
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}