package org.apache.bookkeeper.bookie;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;



@RunWith(value=Parameterized.class)
public class SetLastAddConfirmedTest{
    private FileInfo fi;
    public int remaining;

    private long lac;

    @Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
//          | lac |
            {1    },
        });
    }

    public SetLastAddConfirmedTest(long lac){
        this.lac = lac;
    }

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws Exception {
        fi = mock(FileInfo.class);   
        doCallRealMethod().when(fi).setLastAddConfirmed(lac); 
    }

    @Test
    public void SetLACTest() throws IOException {
        fi.setLastAddConfirmed(lac);
        verify(fi, times(1)).notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, lac);
        fi.setLastAddConfirmed(lac);
        verify(fi, times(1)).notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, lac);
    }
}