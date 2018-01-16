package com.rwj.offlineAnalysisPrj.test.collection.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by renwujie on 2018/01/16 at 11:32
 */
public class TestMap {
    public static void main(String[] args){
        Map<String, Object> map = new HashMap<>();
        //map.put("", "");
        String date = "1";
        System.out.println(map.get(date));
        for(Map.Entry<String, Object> m : map.entrySet()){
            System.out.println(m.getKey() + ":" + m.getValue());
        }
        List<Object> list = new ArrayList<>();
        list.add(0);
        map.put("1", "a");
        map.put("2", "b");
        map.put("1", "c");
        map.put("1", list);
        list.add(2);
        list.add("add");
        for(Map.Entry<String, Object> entry : map.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue());
            if(entry.getValue() instanceof List) {
                List list1 = new ArrayList();
                list1.add("list1");
                map.put("1", list1);
            }
            System.out.println("============================");
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }


    }
}
