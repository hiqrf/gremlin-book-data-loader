package com.bbd.graph;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.metadata.Sheet;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

import javax.ws.rs.NotSupportedException;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.*;

public class ExcelDemoDataLoader {

    private static long initId = 1;

    public static Map<String, Integer> FACTOR = Maps.newHashMap();

    static {
        FACTOR.put("person", 7);
        FACTOR.put("com", 71);
        FACTOR.put("concept", 47251);
        FACTOR.put("legal", 409);
        FACTOR.put("mainarea", 1061);
        FACTOR.put("mainindustry", 1627);
        FACTOR.put("mainIndustry", 1627);
        FACTOR.put("mainproduct", 39323);
        FACTOR.put("product", 4481);
        FACTOR.put("security", 6659000);

    }

    public static void main(String[] args) throws Exception {
        String[] graphAgs = new String[]{"berkeleyje.properties"};
        JanusGraph graph = BaseGraphTool.openGraph(graphAgs);
        loadVertex(graph, args);
        loadEdge(graph, args);

    }

    public static void loadEdge(JanusGraph graph, String[] args) throws Exception {
        Map<String, String> labelAndFileMap = ParamsTool.parseEdgeLabel(args);
        for (Map.Entry<String, String> entry : labelAndFileMap.entrySet()) {
            String label = entry.getKey();
            String file = entry.getValue();
            if (isExcel(file)) {
                List<Object> dataWithHeader = EasyExcelFactory.read(new FileInputStream(new File(file)), new Sheet(1));
                loadEdgeToGraph(graph, label, dataWithHeader, resolveStartLabel(file), resolveEndLabel(file));
            } else if (isCsv(file)) {
                CsvReader reader = new CsvReader(file, ',', Charset.forName("UTF-8"));
                List<Object> dataWithHeader = Lists.newArrayList();
                while (reader.readRecord()) {
                    dataWithHeader.add(Arrays.asList(reader.getValues()));
                }
                reader.close();
                loadEdgeToGraph(graph, label, dataWithHeader, resolveStartLabel(file), resolveEndLabel(file));
            } else {
                throw new NotSupportedException("暂不支持此类文件的导入");
            }
        }
    }

    private static String resolveEndLabel(String file) {
        file = file.split("\\.")[0];//去掉扩展名
        file = file.substring(5, file.length());//去掉edge_
        return file.substring(file.lastIndexOf("\\") + 1, file.length()).split("_")[3];
    }

    private static String resolveStartLabel(String file) {
        file = file.split("\\.")[0];//去掉扩展名
        file = file.substring(5, file.length());//去掉edge_
        return file.substring(file.lastIndexOf("\\") + 1, file.length()).split("_")[1];
    }

    public static void loadVertex(JanusGraph graph, String[] args) throws Exception {
        Map<String, String> labelAndFileMap = ParamsTool.parseVertexLabel(args);
        for (Map.Entry<String, String> entry : labelAndFileMap.entrySet()) {
            String label = entry.getKey();
            String file = entry.getValue();
            if (isExcel(file)) {
                List<Object> dataWithHeader = EasyExcelFactory.read(new FileInputStream(new File(file)), new Sheet(1));
                loadVertexToGraph(graph, label, dataWithHeader);
            } else if (isCsv(file)) {
                CsvReader reader = new CsvReader(file, ',', Charset.forName("UTF-8"));
                List<Object> dataWithHeader = Lists.newArrayList();
                while (reader.readRecord()) {
                    dataWithHeader.add(Arrays.asList(reader.getValues()));
                }
                reader.close();
                loadVertexToGraph(graph, label, dataWithHeader);
            } else {
                throw new NotSupportedException("暂不支持此类文件的导入");
            }
        }
    }

    private static boolean isCsv(String file) {
        return file.endsWith("csv");
    }

    private static boolean isExcel(String file) {
        return file.endsWith("xls") || file.endsWith("xlsx");
    }

    private static void loadEdgeToGraph(JanusGraph graph, String label, List<Object> dataWithHeader, String startLabel, String endLabel) {
        int loop = 0;
        GraphTraversalSource g = graph.traversal();
        List<String> header = (List<String>) dataWithHeader.get(loop++);
        for (; loop < dataWithHeader.size(); loop++) {
            Vertex start = null;
            Vertex end = null;
            List<String> data = (List<String>) dataWithHeader.get(loop);
            List<Object> objects = Lists.newArrayList();
            for (int i = 0; i < header.size(); i++) {
                if (i == 0) {
                    try {
                        start = g.V(BaseGraphTool.getVertexId(Long.parseLong(data.get(i)) * FACTOR.get(startLabel))).next();
                    } catch (NoSuchElementException e) {
                        System.out.println("label:" + startLabel + "orgiid:" + data.get(i));
                        break;
                    } catch (NumberFormatException e) {
                        break;
                    }
                }
                if (i == 1) {
                    try {
                        end = g.V(BaseGraphTool.getVertexId(Long.parseLong(data.get(i)) * FACTOR.get(endLabel))).next();
                    } catch (NoSuchElementException e) {
                        System.out.println("label:" + endLabel + "orgiid:" + data.get(i));
                        break;
                    }catch (NumberFormatException e) {
                        break;
                    }
                }
                objects.add(header.get(i));
                objects.add(Objects.isNull(data.get(i)) ? "" : data.get(i));

            }
            if (Objects.nonNull(start) && Objects.nonNull(end) && !"vertex".equals(start.label())
                    && !"vertex".equals(end.label())) {
                PanguGraphTool.addEdge(start, end, label, objects.toArray(new Object[objects.size()]));
            }
        }
        BaseGraphTool.commit(g.tx());
    }


    private static void loadVertexToGraph(JanusGraph graph, String label, List<Object> dataWithHeader) {
        int loop = 0;
        List<String> header = (List<String>) dataWithHeader.get(loop++);
        JanusGraphTransaction tx = graph.newTransaction();
        for (; loop < dataWithHeader.size(); loop++) {
            long start = System.currentTimeMillis();
            List<String> data = (List<String>) dataWithHeader.get(loop);
            List<Object> objects = Lists.newArrayList();
            objects.add(T.label);
            objects.add(label);
            for (int i = 0; i < header.size(); i++) {
                if (header.get(i).equalsIgnoreCase("id")) {
                    try {
                        objects.add(T.id);
                        objects.add(BaseGraphTool.getVertexId(Long.parseLong(data.get(i)) * FACTOR.get(label)));
                        objects.add("orginalId");
                        objects.add(Long.parseLong(data.get(i)));
                    } catch (NumberFormatException e) {
                        System.out.println("添加label:" + label + "ID 出错" + e);
                        break;
                    }
                } else {
                    objects.add(header.get(i));
                    objects.add(Objects.isNull(data.get(i)) ? "" : data.get(i));
                }
            }
            //只有ID与label
            if (objects.size() < 6) {
                break;
            }
            try {
                tx.addVertex(objects.toArray());
            } catch (Exception e) {
                System.out.println("添加label:" + label + "出错" + e);
            }
            //System.out.println("耗时:" + (System.currentTimeMillis() - start));
        }
        BaseGraphTool.commit(tx);

    }


}
