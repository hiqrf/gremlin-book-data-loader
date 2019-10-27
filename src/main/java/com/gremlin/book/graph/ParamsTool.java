package com.bbd.graph;

import com.google.common.collect.Maps;

import java.util.Map;

public class ParamsTool {

    private ParamsTool() {
    }

    /**
     * 从传入的参数中解析出label与对应数据的文件
     *
     * @param args
     * @return
     */
    public static Map<String, String> parseVertexLabel(String[] args) {

        Map<String, String> lableAndFileMap = Maps.newHashMap();
        for (int i = 0; i < args.length; i += 2) {
            if (args[i].startsWith("--node")) {
                String[] lableAndFile = args[i].split(":");
                String lable = lableAndFile[1];
                String file = args[i + 1];
                lableAndFileMap.put(lable, file);
            }
        }
        return lableAndFileMap;
    }

    /**
     * 从传入的参数中解析出label与对应数据的文件
     *
     * @param args
     * @return
     */
    public static Map<String, String> parseEdgeLabel(String[] args) {

        Map<String, String> lableAndFileMap = Maps.newHashMap();
        for (int i = 0; i < args.length; i += 2) {
            if (args[i].startsWith("--edge")) {
                String[] lableAndFile = args[i].split(":");
                String lable = lableAndFile[1];
                String file = args[i + 1];
                lableAndFileMap.put(lable, file);
            }
        }
        return lableAndFileMap;
    }
}
