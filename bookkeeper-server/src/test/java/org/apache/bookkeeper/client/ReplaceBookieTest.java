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
public class ReplaceBookieTest {

    private String currentEnsemble;
    private List<BookieId> cEns = new ArrayList<>();
    private BookieId toRep;
    private String bookieToReplace;
    private String excludeBookies;
    static Set<BookieId> paramExclude = new HashSet<BookieId>();
    private String knownBookies = "bookie01 bookie02 bookie03 bookie04 bookie05"; 
    private String throwEx;
    private String retStr;
    private DefaultEnsemblePlacementPolicy dEpp;
    private BookieId returnedBookie;
    private int ensembleSize = 1;
    private int ackQuorumSize = 1;
    private int quorumSize = 1;
    private Map<String, byte[]> customMetadata = new HashMap<String, byte[]>();

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{     
//      | currentEnsemble                                | bookieToReplace | excludeBookies                                 | throwEx                       | retStr     |
        { null                                           , "bookie05"      , ""                                             , "NullPointerException"        , null       },
        { "bookie01 bookie05"                            , null            , ""                                             , "IndexOutOfBoundsException"   , null       },  
        { "bookie03"                                     , "bookie03"      , "bookie02 bookie03 bookie04 bookie05"          , null                          , "bookie01" },
        { ""                                             , "bookie03"      , "bookie01"                                     , "IndexOutOfBoundsException"   , null       },
        { "bookie01 bookie02 bookie03 bookie04 bookie05" , "bookie05"      , ""                                             , "BKNotEnoughBookiesException" , null       },
        { "bookie04 bookie05"                            , "bookie05"      , "bookie01 bookie02 bookie03 bookie04 bookie05" , "BKNotEnoughBookiesException" , null       },

    });
    }

    public ReplaceBookieTest(String currentEnsemble, String bookieToReplace, String excludeBookies, String throwEx, String retStr){
        this.currentEnsemble = currentEnsemble;
        this.bookieToReplace = bookieToReplace; 
        this.excludeBookies = excludeBookies;
        this.throwEx = throwEx;
        this.retStr = retStr;
    }

    @Before
    public void replaceBookieSetUp() throws BKNotEnoughBookiesException, UnknownHostException{
        Set<BookieId> toRead = new HashSet<BookieId>();

        dEpp = new DefaultEnsemblePlacementPolicy();

        //Vengono inseriti i known bookies
        Set<BookieId> toWrite = Utility.parser(knownBookies);
        dEpp.onClusterChanged(toWrite, toRead);

        //Si crea il set di bookie da escludere
        if(!excludeBookies.equals("")){
            paramExclude = Utility.parser(excludeBookies);
        }

        //Creazione del current ensemble
        if(currentEnsemble!=null && !currentEnsemble.equals("")){
            cEns.addAll(Utility.parser(currentEnsemble));
        }
        else if(currentEnsemble==null){
            cEns = null;
        }

        //Creazione del BookieId da sostituire
        toRep = Utility.getBookieId(bookieToReplace);

        //Creazione bookie di return
        returnedBookie = Utility.getBookieId(retStr);

    }

    @After
    public void replaceBookieClose(){
        paramExclude = new HashSet<BookieId>();
    }

    @Test
    public void replaceBookieTest() throws UnknownHostException, BKNotEnoughBookiesException{
        try{
            PlacementResult<BookieId> ret = dEpp.replaceBookie(ensembleSize, quorumSize, ackQuorumSize, customMetadata, cEns, toRep, paramExclude);
            Assert.assertEquals(returnedBookie, ret.getResult());
        }catch(Exception e){
            Assert.assertEquals(throwEx, e.getClass().getSimpleName());
        }
    }

}
//onclusterchange initialize(?) updateBookieInfo