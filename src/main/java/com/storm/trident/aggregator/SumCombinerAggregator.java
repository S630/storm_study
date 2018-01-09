/**
 * Project Name:storm_study
 * File Name:Sum.java
 * Package Name:com.storm.trident.aggregator
 * Date:2017年1月5日下午4:12:32
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.aggregator;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import clojure.lang.Numbers;

/**
 * ClassName:Sum <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason:	 TODO ADD REASON. <br/>
 * Date:     2017年1月5日 下午4:12:32 <br/>
 * @author   linux
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class SumCombinerAggregator implements CombinerAggregator<Number> {

    
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;


    public Number init(TridentTuple tuple) {
        return (Number) tuple.getValue(0);
    }

     
    public Number combine(Number val1, Number val2) {
        return Numbers.add(val1, val2);
    }

    
    public Number zero() {
        return 0;
    }
    
}
