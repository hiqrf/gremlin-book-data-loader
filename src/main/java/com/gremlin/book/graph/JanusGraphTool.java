package com.bbd.graph;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class PanguGraphTool {


    private static final Logger logger = LoggerFactory.getLogger(PanguGraphTool.class);


    public static void addEdge(Vertex start, Vertex end, String label, Object... args) {
        List<Object> os = Lists.newArrayList();
        for (int i = 0; i < args.length; i += 2) {
            if (i % 2 == 0 && !Objects.isNull(args[i + 1]) && !"".equals(args[i + 1])) {
                os.add(args[i]);
                os.add(args[i + 1]);
            }
        }
        start.addEdge(label, end, os.toArray(new Object[os.size()]));
    }

}
