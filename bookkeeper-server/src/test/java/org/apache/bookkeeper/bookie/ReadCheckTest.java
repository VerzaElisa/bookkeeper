package org.apache.bookkeeper.bookie;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;



public class ReadCheckTest{
    private FileInfo fi;
    public int remaining;

 ByteBuffer bb;

/*Nel setup viene creato l'oggetto FileInfo.*/
    @Before
    public void setUp() throws Exception {
        bb = ByteBuffer.allocate(1024);
        bb.rewind();

        fi = mock(FileInfo.class);   
        doCallRealMethod().when(fi).read(bb, 0, true); 
    }

    @Test
    public void SetLACTest() throws IOException {
        fi.read(bb, 0, true);
        verify(fi).checkOpen(false);
    }
}