package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.bookkeeper.net.BookieId;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;


@RunWith(value=Parameterized.class)
public class OnClusterChangeTest {

    private String writableBookies;
    private String readOnlyBookies;
    private String throwEx;
    private String retStr;
    private Boolean isWeighted;
    private DefaultEnsemblePlacementPolicy dEpp;
    private String knownBookies = "bookie01 bookie02 bookie03 bookie04 bookie05"; 
    private Map<BookieId, WeightedObject> bookieInfoMap;
    private Map<BookieId, WeightedObject> bookieInfoMapSpy;
    private Set<BookieId> oldBookies;
    private Set<BookieId> diffNew;
    private Set<BookieId> diffDead;
    private WeightedRandomSelection<BookieId> weightedSelection;
    private WeightedRandomSelection<BookieId> weightedSelectionSpy;
    private ReentrantReadWriteLock rwLockMock;
    private WriteLock writeLockMock;
    private Integer t = 0;


    static Set<BookieId> write = new HashSet<BookieId>();
    static Set<BookieId> read = new HashSet<BookieId>();
    static Set<BookieId> ret = new HashSet<BookieId>();


//Mettere i bokie già esistenti in bookieinfomap


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{     
//      | writableBookies                                | readOnlyBookies                                | throwEx                       | retStr                                         | isWeighted |
        { ""                                             , ""                                             , null                          , "bookie01 bookie02 bookie03 bookie04 bookie05" , false      },
        { ""                                             , ""                                             , null                          , "bookie01 bookie02 bookie03 bookie04 bookie05" , true       },
        { null                                           , "bookie06"                                     , "NullPointerException"        , null                                           , false      },  
        { "bookie06"                                     , null                                           , "NullPointerException"        , null                                           , false      },  
        { "bookie01"                                     , "bookie01"                                     , null                          , "bookie02 bookie03 bookie04 bookie05"          , false      },
        { "bookie01 bookie02 bookie03 bookie04 bookie05" , "bookie01 bookie02 bookie03 bookie04 bookie05" , null                          , ""                                             , false      },
        { "bookie01 bookie02 bookie03 bookie04 bookie05" , "bookie01 bookie02 bookie03 bookie04 bookie05" , null                          , ""                                             , true       },
        { "bookie01"                                     , "bookie02"                                     , null                          , "bookie03 bookie04 bookie05"                   , false      },
        { "bookie06"                                     , "bookie01 bookie02 bookie03 bookie04 bookie05" , null                          , ""                                             , true       },
        { "bookie01 bookie07"                            , ""                                             , null                          , "bookie02 bookie03 bookie04 bookie05"          , true       }    
    });
    }

    public OnClusterChangeTest(String writableBookies, String readOnlyBookies, String throwEx, String retStr, Boolean isWeighted){
        this.writableBookies = writableBookies;
        this.readOnlyBookies = readOnlyBookies; 
        this.throwEx = throwEx;
        this.retStr = retStr;
        this.isWeighted = isWeighted;
    }

    @Before
    public void onClusterChangeSetUp() throws BKNotEnoughBookiesException, UnknownHostException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
        dEpp = new DefaultEnsemblePlacementPolicy();
        oldBookies = Utility.parser(knownBookies);
        //Si crea il set di bookie in scrittura e lettura con il valore di ritorno
        if(writableBookies != null){
            write = Utility.parser(writableBookies);
        }
        else{
            write = null;
        }
        if(readOnlyBookies != null){
            read = Utility.parser(readOnlyBookies);
        }
        else{
            read = null;
        }
        if(retStr != null){
            ret = Utility.parser(retStr);
        }
        
        //Si pone a true isWeighted
        if(isWeighted){
            //Calcolo writableBookie-oldBookie
            diffNew = new HashSet<>(write);
            diffNew.removeAll(oldBookies);

            //Calcolo oldBookie-(writableBookie+readableBookie)
            diffDead = new HashSet<>(oldBookies);
            diffDead.removeAll(write);
            diffDead.removeAll(read);

            //Spy di weightedSelection ed inizializzazione
            ClientConfiguration conf = mock(ClientConfiguration.class);
            when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(true);
            when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(10);
            dEpp.initialize(conf, null, null, null, null, null);
            
            weightedSelection = new WeightedRandomSelectionImpl<BookieId>(10);
            weightedSelectionSpy = spy(weightedSelection);
            Field privateField02 = dEpp.getClass().getDeclaredField("weightedSelection");
            privateField02.setAccessible(true);
            privateField02.set(dEpp, weightedSelectionSpy);


            //Spy di bookieInfoMap
            bookieInfoMap = new HashMap<BookieId, WeightedObject>();
            bookieInfoMapSpy = spy(bookieInfoMap);
            Field privateField01 = dEpp.getClass().getDeclaredField("bookieInfoMap");
            privateField01.setAccessible(true);
            privateField01.set(dEpp, bookieInfoMapSpy);
            //if(diffDead.size()>0 || diffNew.size()>0){
            if(Math.max(diffDead.size(), diffNew.size())>0){
                t = 1;
            }

        }

        //Vengono inseriti i known bookies
        Field privateField = dEpp.getClass().getDeclaredField("knownBookies");
        privateField.setAccessible(true);
        privateField.set(dEpp, oldBookies);   
        
        //kill mutation 163
        rwLockMock = mock(ReentrantReadWriteLock.class);
        writeLockMock = Mockito.mock(WriteLock.class);
        Mockito.when(rwLockMock.writeLock()).thenReturn(writeLockMock);

        Field privateField02 = dEpp.getClass().getDeclaredField("rwLock");
        privateField02.setAccessible(true);
        privateField02.set(dEpp, rwLockMock);
    }
    @Test
    public void onClusterChangeTest() throws UnknownHostException, BKNotEnoughBookiesException{
        try{
            Set<BookieId> retBookies = dEpp.onClusterChanged(write, read);
            Assert.assertEquals(true, ret.equals(retBookies));
            verify(writeLockMock).unlock();
            verify(writeLockMock).lock();
            if(isWeighted){
                verify(bookieInfoMapSpy, times(diffDead.size())).remove(any(BookieId.class));
                verify(bookieInfoMapSpy, times(diffNew.size())).put(any(BookieId.class), any(BookieInfo.class));
                //if(Math.max(diffDead.size(), diffNew.size())>0){
                    verify(weightedSelectionSpy, times(t)).updateMap(any(Map.class));
                //}
            }
        }catch(Exception e){
            Assert.assertEquals(throwEx, e.getClass().getSimpleName());
        }
    }
}
