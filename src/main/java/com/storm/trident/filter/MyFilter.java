/**
 * Project Name:storm_study
 * File Name:MyFilter.java
 * Package Name:com.storm.trident.filter
 * Date:2017年1月5日下午3:09:18
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * ClassName:MyFilter <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月5日 下午3:09:18 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class MyFilter extends BaseFilter
{
    public boolean isKeep( TridentTuple tuple )
    {
        return tuple.getInteger( 0 ) == 1 && tuple.getInteger( 1 ) == 2;
    }
}