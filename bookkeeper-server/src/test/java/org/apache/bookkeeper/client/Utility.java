package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.BookieId;

import java.util.HashSet;
import java.util.Set;


public class Utility {
    
    public static Set<BookieId> parser(String toParse){
        Set<BookieId> parsed = new HashSet<BookieId>();

        String[] bookie = toParse.split(" ");
        for(String i : bookie){
            parsed.add(BookieId.parse(i));
        }
        return parsed;
    }

    public static BookieId getBookieId(String bookieStr){
        BookieId toRet;
        if(bookieStr!=null){
            toRet = BookieId.parse(bookieStr);
            return toRet;
        }
        else{
            return null;
        }
    }
}
