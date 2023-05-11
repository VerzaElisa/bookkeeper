package org.apache.bookkeeper.client;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;

import java.util.Set;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.nullable;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;

import io.netty.util.HashedWheelTimer;

@RunWith(value=Parameterized.class)
public class OnClusterChangeTest {

    private String writableBookies;
    private String readOnlyBookies;
    private String throwEx;
    private String retStr;
    private DefaultEnsemblePlacementPolicy dEpp;
    private String knownBookies = "bookie01 bookie02 bookie03 bookie04 bookie05"; 
    static Set<BookieId> write = new HashSet<BookieId>();
    static Set<BookieId> read = new HashSet<BookieId>();
    static Set<BookieId> ret = new HashSet<BookieId>();




    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{     
//      | writableBookies                                | readOnlyBookies                                 | throwEx                       | retStr                                        |
        { ""                                             , ""                                              , null                          , "bookie01 bookie02 bookie03 bookie04 bookie05"},
        { null                                           , "bookie06"                                      , "NullPointerException"        , null                                          },  
        { "bookie06"                                     , null                                            , "NullPointerException"        , null                                          },  
        { "bookie01"                                     , "bookie01"                                      , null                          , "bookie02 bookie03 bookie04 bookie05"         },
        { "bookie01 bookie02 bookie03 bookie04 bookie05" , "bookie02 bookie03 bookie04 bookie05"           , null                          , ""                                            },
        { "bookie01"                                     , "bookie02"                                      , null                          , "bookie03 bookie04 bookie05"                  },

    });
    }

    public OnClusterChangeTest(String writableBookies, String readOnlyBookies, String throwEx, String retStr){
        this.writableBookies = writableBookies;
        this.readOnlyBookies = readOnlyBookies; 
        this.throwEx = throwEx;
        this.retStr = retStr;
    }

    @Before
    public void replaceBookieSetUp() throws BKNotEnoughBookiesException, UnknownHostException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{

        dEpp = new DefaultEnsemblePlacementPolicy();

        //Vengono inseriti i known bookies
        Set<BookieId> oldBookies = Utility.parser(knownBookies);
        Field privateField = dEpp.getClass().getDeclaredField("knownBookies");
        privateField.setAccessible(true);
        privateField.set(dEpp, oldBookies);        
        

        //Si crea il set di bookie in scrittura e lettura e con il valore di ritorno
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
    }
    @Test
    public void onClusterChangeTest() throws UnknownHostException, BKNotEnoughBookiesException{
        try{
            Set<BookieId> retBookies = dEpp.onClusterChanged(write, read);
            Assert.assertEquals(true, ret.equals(retBookies));
        }catch(Exception e){
            Assert.assertEquals(throwEx, e.getClass().getSimpleName());
        }
    }
}
//onclusterchange initialize(?) updateBookieInfo