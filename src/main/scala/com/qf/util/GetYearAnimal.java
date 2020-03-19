package com.qf.util;

public class GetYearAnimal {

    public static String getYear(Integer year){
        if(year<1900){
            return "未知";
        }
        Integer start=1900;
        String [] years=new String[]{
                "鼠","牛","虎","兔",
                "龙","蛇","马","羊",
                "猴","鸡","狗","猪"
        };
        return years[(year-start)%years.length];
    }

}
