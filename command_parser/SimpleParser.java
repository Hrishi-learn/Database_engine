package command_parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class SimpleParser{

    public static Command parser(String key){
        String []tokens = key.split(" ");
        HashMap<String,String>columnValueMap = new HashMap<>();
        String table;
        String rowId;
        List<String>columns = new ArrayList<>();
        Command command = new Command();
        boolean entireRow = false;

        if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.INSERT))){
            int n = tokens.length;
            for(int i=1;i<n-1;i++){
                String[] colValPair = tokens[i].split(":");
                columnValueMap.put(colValPair[0],colValPair[1]);
                columns.add(colValPair[0]);
            }
            table = tokens[n-1];

            command = new Command(CommandType.INSERT,table,"",columnValueMap,columns,false);

        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.DELETE))){
            table = tokens[2];
            rowId = tokens[6];

            command = new Command(CommandType.DELETE,table,rowId,columnValueMap,columns,false);
        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.UPDATE))){
            table = tokens[1];
            columnValueMap.put(tokens[3],tokens[5]);
            rowId = tokens[9];
            columns.add(tokens[3]);

            command = new Command(CommandType.DELETE,table,rowId,columnValueMap,columns,false);
        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.CREATE))){
            if("table".equalsIgnoreCase(tokens[1])){
                table = tokens[2];
                int n = tokens.length;

                for(int i=3;i<n;i++){
                    columns.add(tokens[i]);
                }
            }else {
                table = tokens[3];
                columns.add(tokens[4]);
            }
            command = new Command(CommandType.CREATE,table,"",columnValueMap,columns,false);
        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.SELECT))){
            HashMap<String,Integer>queryStrings = new HashMap<>();
            for(String token:tokens){
                queryStrings.put(token,1);
            }
            if(queryStrings.containsKey("where")){
                if(queryStrings.containsKey("id")){
                    table = tokens[3];
                    rowId = tokens[7];
                    if(tokens[1].equals("*")){
                        entireRow = true;
                    }else{
                        String []parts = tokens[1].split(",");
                        Collections.addAll(columns, parts);
                    }
                }else{
                    table = tokens[3];
                    rowId = "";
                    columnValueMap.put(tokens[5],tokens[7]);
                    columns.add(tokens[5]);
                    if(tokens[1].equals("*")){
                        entireRow = true;
                    }else{
                        String []parts = tokens[1].split(",");
                        Collections.addAll(columns, parts);
                    }
                }
            }else{
                table = tokens[3];
                rowId = "";
                if(tokens[1].equals("*")){
                    entireRow = true;
                }else{
                    String []parts = tokens[1].split(",");
                    Collections.addAll(columns, parts);
                }
            }
            command = new Command(CommandType.SELECT,table,rowId,columnValueMap,columns,entireRow);
        }

        return command;
    }

}
