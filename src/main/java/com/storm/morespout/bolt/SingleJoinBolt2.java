/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.storm.morespout.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TimeCacheMap;

public class SingleJoinBolt2 extends BaseRichBolt
{
    OutputCollector _collector;
    Fields _idFields;
    Fields _outFields;
    int _numSources;
    TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
    Map<String, GlobalStreamId> _fieldLocations;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector )
    {

        _collector = collector;

    }

    public void execute( Tuple tuple )
    {
//        tuple.getValueByField( "str" );
        System.out.println( "************************************" );
        System.out.println( tuple.getFields() );
        System.out.println( tuple.getString( 0 ) + "       " +  tuple.getString( 1 )  );

    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declare( new Fields( "tuplevlaue" ) );
    }

}
