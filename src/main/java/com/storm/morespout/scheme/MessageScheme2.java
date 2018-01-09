/**
 * Project Name:storm_study
 * File Name:MessageScheme.java
 * Package Name:com.storm.morespout.scheme
 * Date:2017年1月20日下午2:19:53
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.morespout.scheme;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * ClassName:MessageScheme <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月20日 下午2:19:53 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class MessageScheme2 implements Scheme
{
    /**
     * serialVersionUID:TODO(用一句话描述这个变量表示什么).
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    public static final String STRING_SCHEME_KEY = "str";

    public List<Object> deserialize( ByteBuffer bytes )
    {
        int base = bytes.arrayOffset();
        String v = new String( bytes.array(), base + bytes.position(), bytes.remaining() );
        
        return new Values(v.split( "," ));
    }

    public static String deserializeString( ByteBuffer string )
    {
        if ( string.hasArray() )
        {
            int base = string.arrayOffset();
            return new String( string.array(), base + string.position(), string.remaining() );
        }
        else
        {
            return new String( Utils.toByteArray( string ), UTF8_CHARSET );
        }
    }

    public Fields getOutputFields()
    {
        return  new Fields("id", "age") ;
    }
}
