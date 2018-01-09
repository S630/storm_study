/**
 * Project Name:storm_study
 * File Name:TestTrident.java
 * Package Name:com.storm.trident.trident
 * Date:2017年1月4日下午10:50:35
 * Copyright (c) 2017, chenzhou1025@126.com All Rights Reserved.
 *
*/

package com.storm.trident.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.storm.trident.filter.FilelterTest;
import com.storm.trident.function.FunctionTest;
import com.storm.trident.function.FunctionTest3;

/**
 * ClassName:TestTrident <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2017年1月4日 下午10:50:35 <br/>
 * 
 * @author linux
 * @version
 * @since JDK 1.6
 * @see
 */
public class TestTrident
{
    @SuppressWarnings( "unchecked" )
    static FixedBatchSpout spout = new FixedBatchSpout( new Fields( "name", "idName", "tel" ), 3, new Values( "Jack", "1", "186107" ), new Values( "Tome", "2", "1514697" ),
            new Values( "Lay", "3", "186745" ), new Values( "Lucy", "4", "1396478" ) );

    @SuppressWarnings( "unchecked" )
    static FixedBatchSpout spout2 = new FixedBatchSpout( new Fields( "sex", "idSex" ), 3, new Values( "Boy", "1" ), new Values( "Boy", "2" ), new Values( "Gril", "3" ),
            new Values( "Gril", "4" ) );

    /**
     * main:(这里用一句话描述这个方法的作用). <br/>
     * TODO(这里描述这个方法适用条件 – 可选).<br/>
     * TODO(这里描述这个方法的执行流程 – 可选).<br/>
     * TODO(这里描述这个方法的使用方法 – 可选).<br/>
     * TODO(这里描述这个方法的注意事项 – 可选).<br/>
     *
     * @author linux
     * @param args
     * @since JDK 1.6
     */
    @SuppressWarnings( "unchecked" )
    public static void main( String[] args )
    {

        // 构建Trident的Topo
        TridentTopology topology = new TridentTopology();

        // // =========================================function测试=========================================start
        // // 设置是否循环
        // FixedBatchSpout spout3 = new FixedBatchSpout( new Fields( "a", "b", "c" ), 3, new Values( 1, 2, 3 ), new
        // Values( 4, 1, 6 ), new Values( 3, 0, 8 ) );
        // spout3.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout3 );
        //
        // Stream st_1 = st.each( new Fields( "b" ), new MyFunction(), new Fields( "d" ) );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "a", "b", "c", "d" ), new FunctionTest(), new Fields( "aa", "bb", "cc" )
        // );
        // // =========================================function测试==========================================end

        // // =========================================Filter测试=========================================start
        //
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b", "c" ), 3, new Values( 1, 2, 3 ), new
        // Values( 2, 1, 1 ), new Values( 2, 3, 4 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.each( new Fields( "b", "a" ), new MyFilter() );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "a", "b", "c" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // // =========================================Filter测试==========================================end

        // ========================================= CombinerAggregator 聚合器的接口 测试
        // 求和=========================================start
        // /**
        // * tuple 求和
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate( new Fields( "b" ), new SumCombinerAggregator(), new Fields( "sum" ) );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "sum" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // ========================================= CombinerAggregator 聚合器的接口测试
        // 求和==========================================end

        // // ======================================= CombinerAggregator 聚合器的接口测试
        // 统计个数=========================================start
        // /**
        // * 统计tuple 的个数
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate ( new Fields( "b" ), new CountCombinerAggregator(), new Fields( "sum" )
        // );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "sum" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // // =========================================CombinerAggregator 聚合器的接口测试
        // 统计个数==========================================end

        // // ======================================= ReducerAggregator 聚合器的接口测试
        // // 统计个数=========================================start
        // /**
        // * 统计tuple 的个数
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate( new Fields( "b" ), new CountReducerAggregator(), new Fields( "sum" ) );
        //
        // 统计个数==========================================end

        // ======================================= ReducerAggregator 聚合器的接口测试
        // 求和========================================start
        // /**
        // * 求和
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate( new Fields( "b" ), new SumReducerAggregator(), new Fields( "sum" ) );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "sum" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // =========================================ReducerAggregator 聚合器的接口测试
        // 求和==========================================end

        // // ======================================= Aggregator 聚合器的接口测试
        // // 求和=========================================start
        // /**
        // * 求和
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate( new Fields( "b" ), new SumAggregator(), new Fields( "sum" ) );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "sum" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // // =========================================Aggregator 聚合器的接口测试
        // // 求和==========================================end

        // ======================================= Aggregator 聚合器的接口测试
        // 统计个数=========================================start
        // /**
        // * 统计个数
        // */
        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b" ), 3, new Values( 1, 2 ), new Values( 2, 1
        // ), new Values( 2, 3 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.partitionAggregate( new Fields( "b" ), new CountAggregator(), new Fields( "sum" ) );
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields( "sum" ), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // =========================================Aggregator 聚合器的接口测试
        // 统计个数==========================================end

        // =======================================projection=========================================start

        // FixedBatchSpout spout4 = new FixedBatchSpout( new Fields( "a", "b", "c" ), 3, new Values( 1, 2, 3 ), new
        // Values( 4, 1, 6 ), new Values( 3, 0, 8 ) );
        //
        // // 设置是否循环
        // spout4.setCycle( false );
        //
        // // 根据spout构建第一个Stream
        // Stream st = topology.newStream( "sp1", spout4 );
        //
        // Stream st_1 = st.project(new Fields("b", "c"));
        //
        // // 对第一个Stream数据显示。
        // Stream st_2 = st_1.each( new Fields("b", "c"), new FunctionTest(), new Fields( "aa", "bb", "cc" ) );
        // =========================================projection==========================================end

        // ====================================join测试==================================================start

        // 设置是否循环
        spout.setCycle( false );

        // 定义过滤器： 电话号码不是 186开头的过滤
        FilelterTest ft = new FilelterTest();
        // 定义方法 用来显示过滤后的数据
        FunctionTest function = new FunctionTest();
        // 根据spout构建第一个Stream
        Stream st = topology.newStream( "sp1", spout );
        // 对第一个Stream数据显示。
        Stream st_1 = st.each( new Fields( "name", "idName", "tel" ), function, new Fields( "out_name", "out_idName", "out_tel" ) );
        // 根据第二个Spout构建Stream，为了测试join用
        Stream st2 = topology.newStream( "sp2", spout2 );
        /**
         * 开始Join st和st2这两个流，类比sql中join的话是： st join st2 on joinFields1 = joinFields2 需要注意的是以st为数据基础 topology.join(st,
         * new Fields("idName"), st2, new Fields("idSex"), new Fields("id","name","tel","sex"))
         * 那么结果将是以spout为数据基础，结果会将上面的4个数据信息全部打出
         */
        Stream st3 = topology.join( st, new Fields( "idName" ), st2, new Fields( "idSex" ), new Fields( "Res_id", "Res_name", "Res_tel", "Res_sex" ) );
        // 创建一个方法，为了显示合并和过滤后的结果
        FunctionTest3 t3 = new FunctionTest3();
        st3.each( new Fields( "Res_id", "Res_name", "Res_tel", "Res_sex" ), ft ).each( new Fields( "Res_id", "Res_name", "Res_tel", "Res_sex" ), t3,
                new Fields( "out1_id", "out1_name", "ou1t_tel", "out1_sex" ) );

        // ====================================join测试==================================================end

        Config cf = new Config();
        // cf.setNumWorkers( 1 );
        // cf.setNumAckers( 0 );
        // cf.setDebug( false );

        LocalCluster lc = new LocalCluster();
        lc.submitTopology( "TestTopo", cf, topology.build() );
    }
}
