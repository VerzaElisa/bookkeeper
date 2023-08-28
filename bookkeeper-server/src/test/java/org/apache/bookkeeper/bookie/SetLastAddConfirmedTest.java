package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.net.BookieId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;
import org.slf4j.event.SubstituteLoggingEvent;
import org.slf4j.impl.StaticLoggerBinder;
import org.slf4j.Logger;
import org.slf4j.event.LoggingEvent;



@RunWith(value=Parameterized.class)
public class SetLastAddConfirmedTest{
    private FileInfo fi;
    private File fl;
    private boolean fileExists;
    private Integer signature;
    private Integer version;
    private Integer lacBufferLen;
    private Integer overflow;
    private int statebits;
    private static byte[] headerMK = "SecondMK".getBytes();
    private Integer headerMKLen;
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