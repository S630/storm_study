
package com.storm.kafka.wordcount;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.storm.utils.Utils;

import kafka.common.Config;

public class WordCountTopology
{

    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main( String[] args ) throws Exception
    {

        // true 集群模式 false本地模式
        boolean local = false;

        TopologyBuilder builder = new TopologyBuilder();

        String zks = "10.253.129.232:2181,10.253.129.233:2181,10.253.129.234:2181,10.253.129.237:2181,10.253.129.238:2181";

        String topic = "you-topic";

        String zkRoot = "/storm"; // default zookeeper root configuration for
                                  // storm
        String id = "you-topic";
        BrokerHosts brokerHosts = new ZkHosts( zks );
        SpoutConfig spoutConf = new SpoutConfig( brokerHosts, topic, zkRoot, id );

        spoutConf.zkServers = Arrays.asList( new String[] {"10.253.129.232", "10.253.129.233", "10.253.129.234", "10.253.129.237", "10.253.129.238"} );
        spoutConf.zkPort = 2181;

        // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setSpout( "kafka-reader", new KafkaSpout( spoutConf ), 5 ); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5

        builder.setBolt( "word-splitter", new KafkaWordSplitter(), 2 ).shuffleGrouping( "kafka-reader" );

        builder.setBolt( "word-counter", new WordCounter() ).fieldsGrouping( "word-splitter", new Fields( "word" ) );

        Map config = new HashMap();

        if ( local )
        {
            try
            {

                // 集群模式
                StormSubmitter.submitTopology( TOPOLOGY_NAME, config, builder.createTopology() );
            }
            catch ( AlreadyAliveException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch ( InvalidTopologyException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            // 本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology( TOPOLOGY_NAME, config, builder.createTopology() );

            Utils.waitForSeconds( 10000 );
            cluster.killTopology( TOPOLOGY_NAME );
            cluster.shutdown();
        }

    }
}
