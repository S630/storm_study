
package com.storm.trident.trident;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

import com.storm.trident.aggregator.CountAgg;
import com.storm.trident.spout.FakeTweetsBatchSpout;
import com.storm.trident.util.Utils;

/**
 * This example is useful for understanding how parallelism and partitioning works. parallelismHit() is applied down
 * until the next partitioning operation. Therefore here we have 5 processes (Bolts) applying a filter and 2 processes
 * creating messages (Spouts).
 * <p>
 * But because we are partitioning by actor and applying a filter that only keeps tweets from one actor, we see in
 * stderr that it is always the same partition who is filtering the tweets, which makes sense.
 * <p>
 * Now comment out the partitionBy() and uncomment the shuffle(), what happens?
 * 
 * @author pere
 */
public class ParallelismExample1
{

    @SuppressWarnings( "serial" )
    public static class PerActorTweetsFilter extends BaseFilter
    {

        private int partitionIndex;
        private String actor;

        public PerActorTweetsFilter( String actor )
        {
            this.actor = actor;
        }

        public void prepare( Map conf, TridentOperationContext context )
        {
            this.partitionIndex = context.getPartitionIndex();
        }

        public boolean isKeep( TridentTuple tuple )
        {
            boolean filter = tuple.getString( 0 ).equals( actor );
            if ( filter )
            {
                System.err.println( "I am partition [" + partitionIndex + "] and I have kept a tweet by: " + actor );
            }
            return filter;
        }
    }

    public static StormTopology buildTopology( LocalDRPC drpc ) throws IOException
    {
        // FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout( 10 );
        //
        // TridentTopology topology = new TridentTopology();
        // topology.newStream( "spout", spout )
        //
        // .shuffle().groupBy( new Fields( "location" ) ).aggregate( new Fields( "location" ), new CountAgg(), new
        // Fields( "count" ) ).parallelismHint( 5 )
        // .each( new Fields( "location", "count" ), new Utils.PrintFilter() );

        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout( 5 );

        TridentTopology topology = new TridentTopology();
//        topology.newStream( "spout", spout ).parallelismHint( 2 ).shuffle().each( new Fields( "actor", "text" ), new PerActorTweetsFilter( "dave" ) ).parallelismHint( 5 )
//                .each( new Fields( "actor", "text" ), new Utils.PrintFilter() );
        
        topology.newStream( "spout", spout ).each( new Fields( "actor" ), new Utils.PrintFilter() ).parallelismHint( 4 );

        return topology.build();

    }

    public static void main( String[] args ) throws Exception
    {
        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "hackaton", conf, buildTopology( drpc ) );
    }
}
