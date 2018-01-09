/**
 * Project Name:storm_study
 * File Name:FunctionTest.java
 * Package Name:com.storm.trident.function
 * Date:2017年1月4日下午10:49:27
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * ClassName:FunctionTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月4日 下午10:49:27 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class FunctionTest extends BaseFunction
{

    
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    public void execute(TridentTuple tuple, TridentCollector collector) {
       
        System.out.println("过滤后数据：============"+tuple.getValues());
      
    }
}
