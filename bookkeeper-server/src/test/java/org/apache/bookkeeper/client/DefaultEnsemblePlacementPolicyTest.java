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
public class DefaultEnsemblePlacementPolicyTest {

    private int ensembleSize;
    private int quorumSize; 
    private int ackQuorumSize;
    private static Map<String, byte[]> customMetadata = new HashMap<String, byte[]>();
    private String excludeBookies;
    private String knownBookies; 
    private String metadata;
    static Set<BookieId> paramExclude = new HashSet<BookieId>();
    private String throwEx;
    private PlacementPolicyAdherence ppa;
    private DefaultEnsemblePlacementPolicy dEpp;


    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{     
//       ensembleSize | quorumSize | ackQuorumSize | customMetadata | excludeBookies      | knownBookies                                   | throwEx                       | placementPolicyAdherence
        {4            , 1          , 1             , "meta value"   , "bookie02 bookie03" , "bookie01 bookie02 bookie03 bookie04 bookie05" , "BKNotEnoughBookiesException" , null},
        {0            , 0          , 0             , "meta value"   , ""                  , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.FAIL},
//      {-1           , -1         , -1            , "meta value"   , ""                  , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.FAIL},
//      {2            , 3          , 1             , "meta value"   , ""                  , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.FAIL},
//      {2            , 1          , 3             , "meta value"   , ""                  , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.FAIL},
        {4            , 3          , 2             , " "             , "bookie05"         , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.MEETS_STRICT},
        {4            , 3          , 2             , null           , "bookie05"          , "bookie01 bookie02 bookie03 bookie04 bookie05" , ""                            , PlacementPolicyAdherence.MEETS_STRICT},

    });
    }

    public DefaultEnsemblePlacementPolicyTest(int ensembleSize, int quorumSize, int ackQuorumSize,
        String metadata, String excludeBookies, String knownBookies, String throwEx, PlacementPolicyAdherence ppa){
        this.ensembleSize = ensembleSize;
        this.quorumSize = quorumSize; 
        this.ackQuorumSize = ackQuorumSize;
        this.metadata = metadata;
        this.excludeBookies = excludeBookies;
        this.knownBookies = knownBookies;
        this.throwEx = throwEx;
        this.ppa = ppa;
    }

    @Before
    public void newEnsembleSetUp() throws BKNotEnoughBookiesException, UnknownHostException{
        Set<BookieId> toWrite = new HashSet<BookieId>();
        Set<BookieId> toRead = new HashSet<BookieId>();

        dEpp = new DefaultEnsemblePlacementPolicy();
        System.out.println("qui");
        //Vengono inseriti i known bookies
        String[] bookie = knownBookies.split(" ");
        for(String i : bookie){
            toWrite.add(BookieId.parse(i));
        }

        dEpp.onClusterChanged(toWrite, toRead);

        //Si crea il set di bookie da escludere
        if(excludeBookies!=""){
            String[] exBookie = excludeBookies.split(" ");
            for(String i : exBookie){
                paramExclude.add(BookieId.parse(i));
            }
        }
        //Si crea la map per i custom metadata
        if(metadata!=" " && metadata!=null){
            String[] meta = metadata.split(" ");
            customMetadata.put(meta[0], meta[1].getBytes());
        }

    }

    @After
    public void newEnsembleClose(){
        paramExclude = new HashSet<BookieId>();
    }

    @Test
    public void newEnsembleTest() throws UnknownHostException, BKNotEnoughBookiesException{
        try{
            PlacementResult<List<BookieId>> ret = dEpp.newEnsemble(ensembleSize, quorumSize, ackQuorumSize, customMetadata, paramExclude);
            Assert.assertEquals(ppa, ret.getAdheringToPolicy());
        }catch(Exception e){
            Assert.assertEquals(throwEx, e.getClass().getSimpleName());
        }
    }

}
