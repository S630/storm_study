/**
 * Project Name:storm_study
 * File Name:Count.java
 * Package Name:com.storm.trident.aggregator
 * Date:2017年1月5日下午3:41:37
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.aggregator;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * ClassName:Count <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月5日 下午3:41:37 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class CountReducerAggregator implements ReducerAggregator<Long>
{
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * 
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    public Long init()
    {

        return 0L;
    }

    public Long reduce( Long curr, TridentTuple tuple )
    {

        return curr + 1;
    }

}
