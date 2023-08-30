package org.apache.bookkeeper.bookie;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;

import org.mockito.Mockito;


@RunWith(value=Parameterized.class)
public class MoveToNewLocationTest{
    private FileInfo fi;
    private static String path = Variables.RLOC_FOLDER+"/moveFile";
    private static String def_path = Variables.RLOC_FOLDER+"/moveFile.rloc";
    private File fl = new File(Variables.TEST_FOLDER+"/"+Variables.LEDGER_FILE_INDEX);
    private File moveFile;
    private String magic = "BKLE";
    private static final int fileLen = 1000;
    private boolean exists;
    private long size;
    private String mException;
    private String fileName;
    private int explicitLacBufLength = 0;
    private long ret;
    private boolean fileChannel;
    private FileChannel fc;
    private FileChannel fc_spy;
    private File moveFileRloc;
    private boolean delete;
    private int times = 1;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | exists | size           | mException  | fileName                                                | fileChannel | delete |
            { false  , 100            , null          , path                                                  , true        , true   }, 
            { true   , 0              , null          , path                                                  , true      , true   },    
            { true   , 1              , null          , path                                                  , true      , true   },
            { true   , 1024+fileLen+1 , null          , path                                                  , true      , true   }, 
            { true   , 1024+fileLen+1 , null          , path                                                  , true      , true   },
            { true   , 1024+fileLen+1 , null          , path                                                  , true      , true   },  
            { true   , 1024+fileLen+1 , null          , path                                                  , true      , true   }, 
            { false  , fileLen-1      , null          , Variables.TEST_FOLDER+"/"+Variables.LEDGER_FILE_INDEX , true      , true   }, 
            { true   , fileLen        , null          , path                                                  , false     , true   },
            { true   , fileLen        , "IOException" , path                                                  , true      , true   },  
            { true   , 1024+fileLen+1 , "IOException" , path                                                  , true      , false  }, 

        }); 
    }

    public MoveToNewLocationTest(boolean exists, long size, String mException, String fileName, boolean fileChannel, boolean delete){
        this.exists = exists;
        this.size = size;
        this.mException = mException;
        this.fileName = fileName;
        this.fileChannel = fileChannel;
        this.delete = delete;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchAlgorithmException {

        ret = size;
        if(fileName.equals(Variables.TEST_FOLDER+"/"+Variables.LEDGER_FILE_INDEX)){
            ret = 1024+fileLen;
        }
        moveFile = new File(fileName);
        byte[] mk = Variables.MASTER_KEY.getBytes();
        int ver = Variables.VERSION;
        if(!delete){
            fl = spy(fl);
            Mockito.when(fl.delete()).thenReturn(delete);
        }
            fi = new FileInfo(fl, mk, ver);

        //Effettuata la spy del file channel
        if(mException!=null && mException.equals("IOException") && delete && fileName != def_path){
            Utilities.createDirectory(Variables.TEST_FOLDER);
            fc = new RandomAccessFile(fl, "rw").getChannel();
            fc_spy = spy(fc);
            Mockito.doReturn(0L).when(fc_spy).transferTo(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(FileChannel.class));
            Utilities.setPrivate(fi, fc_spy, "fc");
        }
        if(fileChannel){
            //Viene scritto l'header e il contenuto randomico del file
            ByteBuffer lac = Utilities.bbCreator(0L, 100L, Math.abs(explicitLacBufLength)).nioBuffer();
            lac.rewind();
            byte[] lac_byte = new byte[lac.remaining()];
            lac.get(lac_byte);
            lac.rewind();
            Utilities.createDirectory(Variables.TEST_FOLDER);
            Utilities.createFile(fl, Variables.MASTER_KEY, magic, 1, Variables.MASTER_KEY.length(), explicitLacBufLength, 0, lac_byte);
            Utilities.writeOnFile(fileLen, fl);
        }else{
            File myObj = new File(Variables.LEDGER_FILE_INDEX); 
            myObj.delete();
            ret = 0;
            times = 0;
        }
        if(exists){
            Utilities.createDirectory(Variables.RLOC_FOLDER);
            moveFileRloc = new File(def_path);
            moveFile.createNewFile();
            moveFileRloc.createNewFile();
        }else{
            Utilities.deleteDirectory(new File(Variables.RLOC_FOLDER));
        }
    }

    @After
    public void onClose(){
        Utilities.deleteDirectory(new File(Variables.TEST_FOLDER)); 
        Utilities.deleteDirectory(new File(Variables.RLOC_FOLDER)); 
    }

    @Test
    public void moveTest(){
        try{
            fi.moveToNewLocation(moveFile, size);
            assertTrue(moveFile.exists());
            Assert.assertEquals(Math.min(ret, 1024+fileLen), moveFile.length());
        }catch(Exception e){ 
            Assert.assertEquals(mException, e.getClass().getSimpleName());
        }
    }
}