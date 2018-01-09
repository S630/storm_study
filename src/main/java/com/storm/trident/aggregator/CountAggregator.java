/**
 * Project Name:storm_study
 * File Name:CountAgg.java
 * Package Name:com.storm.trident.aggregator
 * Date:2017年1月5日下午3:58:56
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.aggregator;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.storm.trident.aggregator.CountAggregator.CountState;

/**
 * ClassName:CountAgg <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年1月5日 下午3:58:56 <br/>
 * @author   linux
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class CountAggregator extends BaseAggregator<CountState> {
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    static class CountState {
        long count = 0;
    }

    public CountState init(Object batchId, TridentCollector collector) {
        return new CountState();
    }

    public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
        state.count+=  tuple.getInteger( 0 ).longValue()  ;
    }

    public void complete(CountState state, TridentCollector collector) {
        collector.emit(new Values(state.count));
    }
}
