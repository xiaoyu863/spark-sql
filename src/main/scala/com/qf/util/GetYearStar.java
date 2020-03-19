package com.qf.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GetYearStar {
    public static String getStar(String dates) {
        Date d=null;
        int point = -1;
        String[] str = {"白羊","金牛","双子","巨蟹","狮子","处女","天平","天蝎","射手","摩羯","水瓶","双鱼"};
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd");

        try {
            d = sdf.parse(dates);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //格式化日期,将日期转成**.**的格式,比如1月1日转成1.01
        Double date = Double.parseDouble((d.getMonth() + 1)
                + "." + String.format("%02d", d.getDate()));

        if (3.21 <= date && 4.19 >= date) {
            point = 0;
        } else if (4.20 <= date && 5.20 >= date) {
            point = 1;
        } else if (5.21 <= date && 6.21 >= date) {
            point = 2;
        } else if (6.22 <= date && 7.22 >= date) {
            point = 3;
        } else if (7.23 <= date && 8.22 >= date) {
            point = 4;
        } else if (8.23 <= date && 9.22 >= date) {
            point = 5;
        } else if (9.23 <= date && 10.23 >= date) {
            point = 6;
        } else if (10.24 <= date && 11.22 >= date) {
            point = 7;
        } else if (11.23 <= date && 12.21 >= date) {
            point = 8;
        } else if (12.22 <= date && 12.31 >= date) {
            point = 9;
        } else if (1.01 <= date && 1.19 >= date) {
            point = 9;
        } else if (1.20 <= date && 2.18 >= date) {
            point = 10;
        } else if (2.19 <= date && 3.20 >= date) {
            point = 11;
        }
        if(point == -1) {
            System.out.println("你真的是地球人么....");
            return "火星人";
        }else{
           return str[point];
        }

    }
}
