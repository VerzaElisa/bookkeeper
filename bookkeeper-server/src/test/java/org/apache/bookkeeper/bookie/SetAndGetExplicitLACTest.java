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
public class SetAndGetExplicitLACTest{
    private FileInfo fi;
    private String exception;
    private int byteBuffLen;
    private File fl = new File(Variables.LEDGER_FILE_INDEX);
    private ByteBuffer bb;
    private Field explicitLacField;
    private ByteBuf retLac;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | byteBuff len | exception                 |                
            { 16           , null                      }, 
            { 24           , null                      },
            { 8            , "BufferUnderflowException"},
        });
    }

    public SetAndGetExplicitLACTest(int byteBuffLen, String exception){
        this.byteBuffLen = byteBuffLen;
        this.exception = exception;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        fi = new FileInfo(fl, mk, ver);
        //Creo bytebuffer con i lac generati casualmente in un range -100 100
        int i = 0;
        long entry;
        long leftLimit = -100L;
        long rightLimit = 100L;
        List<Long> entryList = new ArrayList<>();
        bb = ByteBuffer.allocate(byteBuffLen);
        while(i<(byteBuffLen/8)){
            entry = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
            entryList.add(entry);
            
            bb.putLong(entry);
            i++;
        }
        bb.rewind();

        //Rendo accessibile explicitLac per vedere nel test se è stato settato bene
        explicitLacField = fi.getClass().getDeclaredField("explicitLac");
        explicitLacField.setAccessible(true);

        //Conversione ByteBuffer in ByteBuf
        retLac = Unpooled.buffer(bb.capacity());
        bb.rewind();
        retLac.writeBytes(bb);
        bb.rewind();
    }

    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
    }

    @Test
    public void SAndGExplicitLACTest() throws IOException, IllegalArgumentException, IllegalAccessException {
        try{
            fi.setExplicitLac(retLac);
            retLac.readerIndex(0);
            assertEquals(retLac.nioBuffer(), (ByteBuffer) explicitLacField.get(fi));
            assertEquals(retLac, fi.getExplicitLac());
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}