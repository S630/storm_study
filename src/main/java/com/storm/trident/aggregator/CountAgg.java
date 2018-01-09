/**
 * Project Name:storm_study
 * File Name:CountAgg.java
 * Package Name:com.storm.trident.aggregator
 * Date:2017年1月6日上午9:47:08
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.aggregator;

import java.util.Map;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.storm.trident.aggregator.CountAgg.CountState;

/**
 * ClassName:CountAgg <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年1月6日 上午9:47:08 <br/>
 * @author   linux
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class CountAgg extends BaseAggregator<CountState> {
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;
    
    private int partitionIndex;

    static class CountState {
        long count = 0;
    }
   
    public void prepare( Map conf, TridentOperationContext context )
    {
        
        this.partitionIndex = context.getPartitionIndex();
    }

    public CountState init(Object batchId, TridentCollector collector) {
        System.out.println( "batchId ===  " + batchId );
        return new CountState();
    }

    public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
//        System.err.println( "I am partition [" + partitionIndex + "]" );
        System.out.println( "tuple == "  + tuple);
        state.count+=1;
    }

    public void complete(CountState state, TridentCollector collector) {
        collector.emit(new Values(state.count));
    }
}
