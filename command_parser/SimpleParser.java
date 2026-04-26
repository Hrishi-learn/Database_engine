package command_parser;

import exceptions.InvalidInputException;

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
            if (tokens.length < 3) {
                throw new InvalidInputException("Usage: insert col1:val1 col2:val2 .. tableName");
            }
            int n = tokens.length;
            for(int i=1;i<n-1;i++){
                String[] colValPair = tokens[i].split(":");
                if (colValPair.length != 2) {
                    throw new InvalidInputException("Invalid column format: " + tokens[i] + " expected col:val");
                }
                if (colValPair[0].isEmpty() || colValPair[1].isEmpty()) {
                    throw new InvalidInputException("Column name or value cannot be empty: " + tokens[i]);
                }
                columnValueMap.put(colValPair[0],colValPair[1]);
                columns.add(colValPair[0]);
            }
            table = tokens[n-1];

            command = new Command(CommandType.INSERT,table,"",columnValueMap,columns,false);

        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.DELETE))){
            if (tokens.length != 7) {
                throw new InvalidInputException("Usage: delete from tableName where id = ?");
            }

            table = tokens[2];
            rowId = tokens[6];

            try {
                Integer.parseInt(rowId);
            } catch (NumberFormatException e) {
                throw new InvalidInputException("Row id must be a number, got: " + rowId);
            }

            command = new Command(CommandType.DELETE,table,rowId,columnValueMap,columns,false);
        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.UPDATE))){
            if (tokens.length != 10) {
                throw new InvalidInputException("Usage: update tableName set column = value where id = ?");
            }
            table = tokens[1];
            columnValueMap.put(tokens[3],tokens[5]);
            rowId = tokens[9];
            columns.add(tokens[3]);
            try {
                Integer.parseInt(rowId);
            } catch (NumberFormatException e) {
                throw new InvalidInputException("Row id must be a number, got: " + rowId);
            }

            command = new Command(CommandType.DELETE,table,rowId,columnValueMap,columns,false);
        }else if(tokens[0].equalsIgnoreCase(String.valueOf(CommandType.CREATE))){
            if("table".equalsIgnoreCase(tokens[1])){
                table = tokens[2];
                int n = tokens.length;
                if (n < 4) {
                    throw new InvalidInputException("Usage: create table tableName col1 col2 col3");
                }
                if (table.isEmpty()) {
                    throw new InvalidInputException("Table name cannot be empty");
                }

                for(int i=3;i<n;i++){
                    columns.add(tokens[i]);
                }
            }else {
                if (tokens.length != 5 || !tokens[2].equalsIgnoreCase("ON")) {
                    throw new InvalidInputException("Usage: create index on tableName columnName");
                }
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
                if(tokens.length!=8){
                    throw new InvalidInputException("Usage: select * from tableName");
                }
                if(queryStrings.containsKey("id")){
                    table = tokens[3];
                    rowId = tokens[7];
                    try {
                        Integer.parseInt(rowId);
                    } catch (NumberFormatException e) {
                        throw new InvalidInputException("Row id must be a number, got: " + rowId);
                    }
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
                if (tokens.length != 4) {
                    throw new InvalidInputException("Usage: select * from tableName");
                }
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
