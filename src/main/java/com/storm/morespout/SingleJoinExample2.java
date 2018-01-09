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

package com.storm.morespout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.storm.morespout.bolt.SingleJoinBolt2;
import com.storm.morespout.exmple.SingleJoinBolt;
import com.storm.morespout.scheme.MessageScheme;
import com.storm.morespout.scheme.MessageScheme2;
import com.storm.utils.Utils;

public class SingleJoinExample2
{
    public static void main( String[] args ) throws InterruptedException
    {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts brokerHosts = new ZkHosts( "10.31.23.77:2181" );

        // ********************genderSpout**************************** start
        SpoutConfig spoutConf = new SpoutConfig( brokerHosts, "ysj_topic1", "/storm", "ysj_topic1" );

        spoutConf.scheme = new SchemeAsMultiScheme( new MessageScheme() );

        spoutConf.maxOffsetBehind = Long.MAX_VALUE;
        // -2: 从最老的开始读 -1: 从最近的开始读 0: 从Zk中读
        spoutConf.startOffsetTime = -1;

        KafkaSpout genderSpout = new KafkaSpout( spoutConf );

        // ********************genderSpout**************************** end
        
        // ********************genderSpout**************************** start
        SpoutConfig spoutConf2 = new SpoutConfig( brokerHosts, "ysj_topic2", "/storm", "ysj_topic2" );

        spoutConf2.scheme = new SchemeAsMultiScheme( new MessageScheme2() );

        spoutConf2.maxOffsetBehind = Long.MAX_VALUE;
        // -2: 从最老的开始读 -1: 从最近的开始读 0: 从Zk中读
        spoutConf2.startOffsetTime = -1;

        KafkaSpout ageSpout = new KafkaSpout( spoutConf2 );

        // ********************genderSpout**************************** end

        builder.setSpout( "gender", genderSpout, 4 );
        builder.setSpout( "age", ageSpout, 4 );

//        builder.setBolt( "join", new SingleJoinBolt2() ).shuffleGrouping( "age" );
        builder.setBolt("join", new SingleJoinBolt(new Fields("id","gender", "age"))).fieldsGrouping("gender", new Fields("id"))
        .fieldsGrouping("age", new Fields("id"));

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();

        conf.setDebug( true );

        cluster.submitTopology( "join-example", conf, builder.createTopology() );

        Utils.sleep( 200000 );
        cluster.shutdown();
        // cluster.shutdown();
    }
}
