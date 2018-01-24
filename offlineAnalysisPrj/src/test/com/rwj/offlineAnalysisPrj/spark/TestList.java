package com.rwj.offlineAnalysisPrj.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by renwujie on 2018/01/18 at 10:41
 */
public class TestList {
    public static void main(String[] args){

        /**
         * list
         */
        List<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(1);
        System.out.println(list.toString());

        /**
         * 基本数据类型
         */
        //这就是为什么最后总是不够100条的原因。
        System.out.println((int)5.56);
        System.out.println((int)5.53);

        /**
         * Iterator
         *  下面这段代码是个死循环，当 it.hasNext()==true 进去后，直接判断了 list.comtains(index)， 此时index=0，
         */
        Iterator<Integer> it = list.iterator();
        while(it.hasNext()) {
            int index = 0;
            //list 1 2 3
            if(list.contains(index)) {
                //TODO:必须要手动.next()才能将指针向下一位移动，否则就是死循环跳不出去了。
                //System.out.println(it.next());
                System.out.println("___");
                //TODO:放在这里出不去，因为index永远是0 && 指针永远在第一个位置。
                //it.next();
            }
            //TODO:放在这里是可以结束的，因为while()成 false 了
            it.next();
            index++;
        }

    }
}
