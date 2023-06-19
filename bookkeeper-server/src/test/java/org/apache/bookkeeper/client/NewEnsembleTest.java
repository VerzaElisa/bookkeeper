package org.apache.bookkeeper.client;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.Set;

import org.apache.bookkeeper.net.BookieId;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value=Parameterized.class)
public class NewEnsembleTest {

    private int ensembleSize;
    private int quorumSize; 
    private int ackQuorumSize;
    private static Map<String, byte[]> customMetadata = new HashMap<String, byte[]>();
    private String excludeBookies;
    private String knownBookies = "bookie01 bookie02 bookie03 bookie04 bookie05"; 
    private String metadata;
    private Boolean isWeighted;
    static Set<BookieId> paramExclude = new HashSet<BookieId>();
    static Set<BookieId> oldBookies = new HashSet<BookieId>();
    private String throwEx;
    private PlacementPolicyAdherence ppa;
    private DefaultEnsemblePlacementPolicy dEpp;



    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{     
//      | ensembleSize | quorumSize | ackQuorumSize | customMetadata | excludeBookies      | throwEx                       | placementPolicyAdherence              | isWeighted |
        { 4            , 1          , 1             , "meta value"   , "bookie02 bookie03" , "BKNotEnoughBookiesException" , null                                  , false      },
        { 4            , 1          , 1             , "meta value"   , "bookie02 bookie03" , "BKNotEnoughBookiesException" , null                                  , true       },
        { 0            , 0          , 0             , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , false      },
//      { -1           , -1         , -1            , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , false      },
//      { 2            , 3          , 1             , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , false      },
//      { 2            , 1          , 3             , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , false      },
//      { 2            , 3          , 1             , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , true       },      
//      { 2            , 1          , 3             , "meta value"   , ""                  , ""                            , PlacementPolicyAdherence.FAIL         , true      },
        { 4            , 3          , 2             , " "            , "bookie05"          , ""                            , PlacementPolicyAdherence.MEETS_STRICT , true       },
        { 4            , 3          , 2             , " "            , "bookie05"          , ""                            , PlacementPolicyAdherence.MEETS_STRICT , false      },
        { 4            , 3          , 2             , null           , "bookie05"          , ""                            , PlacementPolicyAdherence.MEETS_STRICT , false      },

    });
    }

    public NewEnsembleTest(int ensembleSize, int quorumSize, int ackQuorumSize,
        String metadata, String excludeBookies, String throwEx, PlacementPolicyAdherence ppa, Boolean isWeighted){
        this.ensembleSize = ensembleSize;
        this.quorumSize = quorumSize; 
        this.ackQuorumSize = ackQuorumSize;
        this.metadata = metadata;
        this.excludeBookies = excludeBookies;
        this.throwEx = throwEx;
        this.ppa = ppa;
        this.isWeighted = isWeighted;
    }

    @Before
    public void newEnsembleSetUp() throws BKNotEnoughBookiesException, UnknownHostException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException{

        dEpp = new DefaultEnsemblePlacementPolicy();
        //Vengono inseriti i known bookies
        oldBookies = Utility.parser(knownBookies);

        Field privateField = dEpp.getClass().getDeclaredField("knownBookies");
        privateField.setAccessible(true);
        privateField.set(dEpp, oldBookies);        
        
        //Si crea il set di bookie da escludere
        if(excludeBookies!=""){
            paramExclude = Utility.parser(excludeBookies);
        }

        //Si crea la map per i custom metadata
        if(metadata!=" " && metadata!=null){
            String[] meta = metadata.split(" ");
            customMetadata.put(meta[0], meta[1].getBytes());
        }

        //Si pone a true la variabile isWeighted
        if(isWeighted){
            //Viene chiamato il metodo initialize per inizializzare la variabile weightedSelection che estrae i bookie per l'ensemble
            ClientConfiguration conf = mock(ClientConfiguration.class);
            when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(true);
            when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(10);
            dEpp.initialize(conf, null, null, null, null, null);
        
            //Costruzione del pool di bookie id tra cui scegliere
            List<BookieId> arr = new ArrayList<>();
            for(int i = 0; i<4; i++){
                arr.add(BookieId.parse("bookie-"+Math.random()));
            }
            //Viene mockata la weightedSelection per fare in modo che prenda i bookie da un set predisposto che presenta inizialmente due bookie uguali
            //poi un bookie id da escludere e poi altri tre bookie validi
            WeightedRandomSelectionImpl<BookieId> wrs = mock(WeightedRandomSelectionImpl.class);
            when(wrs.getNextRandom()).thenReturn(arr.get(0), arr.get(0), paramExclude.iterator().next(), arr.get(1), arr.get(2), arr.get(3));
            Field weightedSelection = dEpp.getClass().getDeclaredField("weightedSelection");
            weightedSelection.setAccessible(true);
            weightedSelection.set(dEpp, wrs);

            //Costruzione set di ritorno dei test
            oldBookies = new HashSet<BookieId>();
            for(int i = 0; i<ensembleSize;i++){
                oldBookies.add(arr.get(i));
            }
        }
        else{
            //Si crea il set contenente i knownBookie - excludeBookie
            oldBookies.removeAll(paramExclude);
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
            if(ppa.equals(PlacementPolicyAdherence.MEETS_STRICT)){
                Assert.assertTrue(oldBookies.containsAll(ret.getResult()));
            }
        }catch(Exception e){
            Assert.assertEquals(throwEx, e.getClass().getSimpleName());
        }
    }
}
