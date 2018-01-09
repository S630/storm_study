/**
 * Project Name:storm_study
 * File Name:FunctionTest3.java
 * Package Name:com.storm.trident.function
 * Date:2017年1月4日下午10:50:08
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * ClassName:FunctionTest3 <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月4日 下午10:50:08 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class FunctionTest3 extends BaseFunction
{

   
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    public void execute( TridentTuple tuple, TridentCollector collector )
    {

        System.out.println( tuple.getValues() );
    }

}
