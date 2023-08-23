package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.net.BookieId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private boolean full;
    private int inside;
    private Level logStatus;
    private Logger LOG;
    private Logger LOG_SPY;
    private ByteBuf isNull = null;
    private ByteBuf existing;
    private ByteBuffer existingBB;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | byteBuff len | exception                  | full  | inside | logStatus   |
            { 16           , null                       , true  , 16     , Level.INFO  },
            //{ 16           , null                       , true  , 0    },    
            { 16           , null                       , false , 0      , Level.DEBUG },
            { 24           , null                       , false , 0      , Level.DEBUG },
            { 8            , "BufferUnderflowException" , false , 0      , Level.DEBUG },
        });
    }

    public SetAndGetExplicitLACTest(int byteBuffLen, String exception, Boolean full, int inside, Level logStatus){
        this.byteBuffLen = byteBuffLen;
        this.exception = exception;
        this.full = full;
        this.inside = inside;
        this.logStatus = logStatus;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws Exception {
        Configurator.setLevel("org.apache.bookkeeper.bookie.FileInfo", logStatus);
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;

        //Inserimento logger spy
        LOG = LoggerFactory.getLogger(FileInfo.class);
        LOG_SPY = spy(LOG);

        Utilities.setFinalStatic(FileInfo.class.getDeclaredField("LOG"), LOG_SPY);
        fi = new FileInfo(fl, mk, ver);
        //Creo bytebuffer con i lac generati casualmente in un range -100 100
        retLac = Utilities.bbCreator(0L, 100L, byteBuffLen);
        //Rendo accessibile explicitLac per vedere nel test se è stato settato bene
        explicitLacField = fi.getClass().getDeclaredField("explicitLac");
        explicitLacField.setAccessible(true);
        //Se richiesto dal test viene inizializzato explicitLac
        if(full){
            existing = Utilities.bbCreator(101L, 200L, inside);
            existingBB = existing.nioBuffer();
            explicitLacField.set(fi, existingBB);
        }

    }

    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
    }

    @Test
    public void SAndGExplicitLACTest() throws IOException, IllegalArgumentException, IllegalAccessException {
        if(!full){
            ByteBuf ret = fi.getExplicitLac();
            Assert.assertEquals(null, ret);
            verify(LOG_SPY).debug("fileInfo:GetLac: {}", isNull);
        }
        try{
            fi.setExplicitLac(retLac);
            retLac.readerIndex(0);
            if(logStatus.equals(Level.DEBUG)){
                verify(LOG_SPY).debug("fileInfo:SetLac: {}", retLac.nioBuffer());
            }
            assertEquals(retLac.nioBuffer(), (ByteBuffer) explicitLacField.get(fi));
            assertEquals(retLac, fi.getExplicitLac());
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}