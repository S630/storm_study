
package com.storm.trident.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Misc. util classes that can be used for implementing some stream processing examples.
 * 
 * @author pere
 */
public class Utils
{

    /**
     * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
     */
    @SuppressWarnings( {"serial", "rawtypes"} )
    public static class PrintFilter implements Filter
    {

        private int partitionIndex;
        
        private int num ;
        public void prepare( Map conf, TridentOperationContext context )
        {
            this.partitionIndex = context.getPartitionIndex();
            num = context.numPartitions();
        }

        public void cleanup()
        {
        }

        public boolean isKeep( TridentTuple tuple )
        {
            System.out.println( tuple  + ",partitionIndex = " + partitionIndex + ",num" + num);
            return true;
        }
    }

    /**
     * Given a hashmap with string keys and integer counts, returns the "top" map of it. "n" specifies the size of the
     * top to return.
     */
    public final static Map<String, Integer> getTopNOfMap( Map<String, Integer> map, int n )
    {
        List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>( map.size() );
        entryList.addAll( map.entrySet() );
        Collections.sort( entryList, new Comparator<Map.Entry<String, Integer>>()
        {

            public int compare( Entry<String, Integer> arg0, Entry<String, Integer> arg1 )
            {
                return arg1.getValue().compareTo( arg0.getValue() );
            }
        } );
        Map<String, Integer> toReturn = new HashMap<String, Integer>();
        for ( Map.Entry<String, Integer> entry : entryList.subList( 0, Math.min( entryList.size(), n ) ) )
        {
            toReturn.put( entry.getKey(), entry.getValue() );
        }
        return toReturn;
    }
}
