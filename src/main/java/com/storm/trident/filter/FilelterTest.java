/**
 * Project Name:storm_study
 * File Name:FilelterTest.java
 * Package Name:com.storm.trident.filter
 * Date:2017年1月4日下午10:47:24
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * ClassName:FilelterTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月4日 下午10:47:24 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class FilelterTest extends BaseFilter
{

     
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;

    public boolean isKeep( TridentTuple tuple )
    {
        String get = tuple.getString( 2 );
        System.out.println( "============================" );
        System.out.println( tuple );
        if ( get.startsWith( "186" ) )
        {
            return true;
        }
        return false;
    }
}
