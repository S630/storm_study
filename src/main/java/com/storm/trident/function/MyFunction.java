/**
 * Project Name:storm_study
 * File Name:MyFunction.java
 * Package Name:com.storm.trident.function
 * Date:2017年1月5日下午2:56:49
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * ClassName:MyFunction <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月5日 下午2:56:49 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class MyFunction extends BaseFunction
{
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    public void execute( TridentTuple tuple, TridentCollector collector )
    {
        for ( int i = 0; i < tuple.getInteger( 0 ); i++ )
        {
            collector.emit( new Values( i ) );
        }
    }
}
