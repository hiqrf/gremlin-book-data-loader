package com.bbd.graph;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseGraphTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseGraphTool.class);


    private static JanusGraph graph = null;

    public static long getVertexId(long id) {
        return (id << 8) | ((id & 0x1F) << 3);
    }

    public synchronized static JanusGraph openGraph(String[] args) throws Exception {
        if (graph == null) {
            PropertiesConfiguration janusgraphConf = new PropertiesConfiguration(
                    args[0]);
            Iterator<String> lt = janusgraphConf.getKeys();
            while (lt.hasNext()) {
                String key = lt.next();
                LOGGER.info(key + " : " + janusgraphConf.getProperty(key));
            }
            LOGGER.info("begin to open graph...");
            graph = (JanusGraph) GraphFactory.open(janusgraphConf);
            LOGGER.info("open graph successfully.");
        }
        return graph;
    }

    public synchronized static JanusGraph existsGraph() {
        if (graph == null) {
            throw new RuntimeException("没有存在的图");
        }
        return graph;
    }


    public static void commit(Transaction tx) {
        tx.commit();
        LOGGER.info("commit transaction successfully.");
    }

    public static void commit(JanusGraphTransaction tx) {
        tx.commit();
        LOGGER.info("commit transaction successfully.");
    }

    public synchronized static void closeGraph() {
        LOGGER.info("begin to close graph...");
        if (graph != null) {
            graph.close();
            graph = null;
        }
        LOGGER.info("close graph successfully.");
    }

    public static void handleException(JanusGraphTransaction tx,
                                       Exception e) {
        LOGGER.error(e.getMessage(), e);
        try {
            tx.rollback();
        } catch (Exception e1) {
            LOGGER.error(e1.getMessage(), e1);
        }
    }

    public static void handleException(Transaction tx,
                                       Exception e) {
        LOGGER.error(e.getMessage(), e);
        try {
            tx.rollback();
        } catch (Exception e1) {
            LOGGER.error(e1.getMessage(), e1);
        }
    }

    public static Parameter getMapping(String[] es) {
        if (es.length < 3) {
            return Mapping.DEFAULT.asParameter();
        }
        if ("TEXT".equals(es[2])) {
            return Mapping.TEXT.asParameter();
        }
        if ("STRING".equals(es[2])) {
            return Mapping.STRING.asParameter();
        }
        if ("TEXTSTRING".equals(es[2])) {
            return Mapping.TEXTSTRING.asParameter();
        }
        if ("PREFIX_TREE".equals(es[2])) {
            return Mapping.PREFIX_TREE.asParameter();
        }
        return Mapping.DEFAULT.asParameter();
    }


    public static void main(String[] args) throws Exception {
        // args = new String[]{"janusgraph-hbase-es-ddb-prod_master.properties"};
        JanusGraph graph = openGraph(args);
        GraphTraversalSource g = graph.traversal();
        System.out.println("begin");

        //通联场景
        // Set<Object> phoneIds = g.V().has("Person","zjhm","500382198610028277").outE("Smz").id().toSet();
        // List<Object> otherPhones = phoneIds.parallelStream().map(phoneid -> g.V(phoneid).both().id().toSet())
        //       .flatMap(ids -> ids.parallelStream()).collect(Collectors.toList());
        // List<Object> i = new BlockingArrayQueue<>();
        // List<List<Object>> otherPhoness = Lists.partition(otherPhones, 100);
        // List<Object> otherPerson = otherPhoness.parallelStream().flatMap(ps->ps.parallelStream()).map(otherPhone -> g.V(otherPhone).in("Smz").id().toSet())
        //       .flatMap(ids -> ids.parallelStream()).collect(Collectors.toList());

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            List<Object> edges = g.V().has("Phone", "phone", "13883883417").bothE("Allcall").as("edge")
                    .otherV().has("Phone", "phone", "13274900413")
                    .select("edge").toList();
            // Object id1 = g.V().has("Person", "zjhm", "500382198610028277").limit(1).id().next();
            // Object id2 = g.V().has("Person", "zjhm", "500382198612128642").limit(1).id().next();
            // Set<Map<String, Object>> s = g.V(id1).bothE().as("edge").otherV().as("v").hasId(id2).select("edge", "v").toSet();
            System.out.println("ID loop：" + i + "耗时:" + (System.currentTimeMillis() - start) + "");
            g.tx().rollback();
        }
        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            Vertex v1 = g.V().has("Person", "zjhm", "500382198610028277").limit(1).next();
            Vertex v2 = g.V().has("Person", "zjhm", "500382198612128642").limit(1).next();
            Set<Map<String, Object>> s = g.V(v1).bothE().as("edge").otherV().hasId(v2.id()).select("edge", "v").toSet();
            System.out.println("vertex loop：" + i + "耗时:" + (System.currentTimeMillis() - start) + "");
            g.tx().rollback();
        }
        System.out.println("end");


    }
}
