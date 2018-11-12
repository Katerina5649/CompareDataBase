package com.company;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class users_database {



    public static boolean compareList (List<Map<String, String>> ds1, List<Map<String, String>> ds2){
        if (ds1.size() != ds2.size()) return false;

        AtomicBoolean flag = new AtomicBoolean(true);

        ds1.forEach(i -> {
            if (!ds2.remove(i)){ flag.set(false); }
        });

        return flag.get();
    }


    public static boolean isNumeric(String string) {
        try {
            double d = Double.parseDouble(string);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    /*
нет набора полей -  по умолчанию false

     */
    public boolean compareDataBase(String user, String passwd, String dataBase_1, String dataBase_2, String table_1, String table_2, String... column) throws SQLException {

        boolean flag = false;
        if (column.length == 0)
            return false;

        Connection conn1 = null;
        Connection conn2 = null;
        try {

            conn1 = DriverManager.getConnection(
                    dataBase_1,
                    user, passwd);
            conn2 = DriverManager.getConnection(
                    dataBase_2,
                    user, passwd);

            if (conn1 == null) {
                System.out.println("Нет соединения с БД!" + dataBase_1);
                System.exit(0);
            }


            if (conn2 == null) {
                System.out.println("Нет соединения с БД!" + dataBase_2);
                System.exit(0);
            }
            Statement stmt1 = conn1.createStatement();
            Statement stmt2 = conn2.createStatement();

            ResultSet resultSet1 = stmt1.executeQuery("SELECT "+column + " FROM "+ table_1);
            ResultSet resultSet2 = stmt2.executeQuery("SELECT "+column+" FROM " + table_2);


            ArrayList<Map<String, String>> list_1 = new ArrayList<>();
            ArrayList<Map<String, String>> list_2 = new ArrayList<>();

            //  map_1.stream();
            int i;
            while(resultSet1.next()){
                HashMap<String, String> map_1 = new HashMap<>();
                for ( i = 0; i < column.length; i++) {

                    map_1.put(column[i], resultSet1.getString(column[i]));
                }
                list_1.add(map_1);
            }
            while(resultSet2.next()){
                HashMap<String, String> map_2 = new HashMap<>();
                for ( i = 0; i < column.length; i++) {
                    map_2.put(column[i], resultSet2.getString(column[i]));
                }
                list_1.add(map_2);
            }


            flag =  compareList(list_1, list_2);




        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn1 != null)
                conn1.close();
            if (conn1 != null)
                conn1.close();


        }
        return flag;
    }

}