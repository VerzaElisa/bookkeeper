package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.common.util.Watcher;
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;



@RunWith(value=Parameterized.class)
public class SetAndWaitLACTest{
    private FileInfo fi;
    private File fl;
    public int remaining;

    private long lac;
    private String lacStr;
    private Level logStatus;
    private String elem = "";
    private List<Long> parsed;
    private List<Long> exp;
    private static Watcher<LastAddConfirmedUpdateNotification> watcher;
    private boolean wait;
    private long cur;
    private String exception;
    private long prevLac;
    private Logger LOG;
    private Logger LOG_SPY;
    private boolean isClosed;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | LAC | LAC list | exception              | watcher | wait  | logStatus   | isClosed |
            {-1   , "3 2 4"  , null                   , watcher , false , Level.TRACE , false    }, 
            {0    , "3 2 4"  , null                   , watcher , true  , Level.INFO  , false    },
            {1    , "3 2 4"  , "NullPointerException" , null    , true  , Level.INFO  , false    },
            {0    , ""       , null                   , watcher , false , Level.INFO  , true     },
        });
    }

    public SetAndWaitLACTest(long lac, String lacStr, String exception, Watcher<LastAddConfirmedUpdateNotification> watcher, boolean wait, Level logStatus, boolean isClosed){
        this.lac = lac;
        this.lacStr = lacStr;
        SetAndWaitLACTest.watcher = watcher;
        this.wait = wait;
        this.exception = exception;
        this.logStatus = logStatus;
        this.isClosed = isClosed;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws Exception {
        Configurator.setLevel("org.apache.bookkeeper.bookie.FileInfo", logStatus);
        byte[] mk = Variables.MASTER_KEY.getBytes();
        fl = new File(Variables.LEDGER_FILE_INDEX);
        int ver = Variables.VERSION;

        //Inserimento logger spy
        LOG = LoggerFactory.getLogger(FileInfo.class);
        LOG_SPY = spy(LOG);
        Utilities.setFinalStatic(FileInfo.class.getDeclaredField("LOG"), LOG_SPY);
        fi = new FileInfo(fl, mk, ver);   
         
        //Creazione lista invocazioni successive di setLasAddConfirmed
        if(!lacStr.equals("")){
            expBuilding();
        }
        //Creato mock per waitForLastAddConfirmedUpdate
        watcher = Mockito.mock(Watcher.class);

        //Modifica parametro isClosed
        Utilities.setPrivate(fi, isClosed, "isClosed");
    }

    public void expBuilding(){
        parsed = new ArrayList<Long>();
        exp = new ArrayList<Long>();
        parsed = new ArrayList<Long>();
        String[] lacList = lacStr.split(" ");
        for(String i : lacList){
            long strToLong = Long.parseLong(i);
            parsed.add(strToLong);
            if(elem == null){
                cur = strToLong;
                exp.add(strToLong);
            }
            else{
                cur = Math.max(strToLong, cur);
                exp.add(cur);
            }
            System.out.println(exp);
        }
        //Valore di ritorno per il test su waitForLastAddConfirmedUpdate
        prevLac = cur;
        if(!wait){
            prevLac--;
        }
    }

    

    @After
    public void onClose(){
        File myObj = new File(Variables.LEDGER_FILE_INDEX); 
        myObj.delete();
    }
    @Test
    public void SAndWLACTest() throws IOException {
        if(!lacStr.equals("")){
            long ret = fi.setLastAddConfirmed(lac);
            assertEquals(lac, ret);
            for(int i = 0; i<parsed.size(); i++){
                assertEquals((long)exp.get(i), fi.setLastAddConfirmed(parsed.get(i)));
                if(logStatus.equals(Level.TRACE)){
                    verify(LOG_SPY).trace("Updating LAC {} , {}", exp.get(i), parsed.get(i));
                }
            }
        }
        try{
            assertEquals(wait, fi.waitForLastAddConfirmedUpdate(prevLac, watcher));
            if(logStatus.equals(Level.TRACE)){
                verify(LOG_SPY).trace("Wait For LAC {} , {}", exp.get(exp.size()-1), prevLac);
            }
        }catch(Exception e){    
            Assert.assertEquals(exception, e.getClass().getSimpleName());
        }
    }
}