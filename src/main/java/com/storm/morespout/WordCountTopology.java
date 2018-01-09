
package com.storm.morespout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.morespout.bolt.SplitSentenceBolt;
import com.storm.morespout.spout.SentenceSpout;
import com.storm.morespout.spout.SentenceSpout2;
import com.storm.utils.Utils;

public class WordCountTopology
{

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SENTENCE_SPOUT_ID2 = "sentence-spout2";
    
    private static final String SPLIT_BOLT_ID = "split-bolt";

    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main( String[] args ) throws Exception
    {

        // true 集群模式 false本地模式
        boolean local = false;
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        
        SentenceSpout2 spout2 = new SentenceSpout2();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout( SENTENCE_SPOUT_ID, spout );
        
        builder.setSpout( SENTENCE_SPOUT_ID2, spout2 );

        builder.setBolt( SPLIT_BOLT_ID, splitBolt ).shuffleGrouping( SENTENCE_SPOUT_ID ).shuffleGrouping( SENTENCE_SPOUT_ID2 );

        Config config = new Config();
        // config.setMessageTimeoutSecs( secs );

        if ( local )
        {
            try
            {
                config.setNumWorkers( 2 );
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

            Utils.waitForSeconds( 10 );
            cluster.killTopology( TOPOLOGY_NAME );
            cluster.shutdown();
        }
    }
}
