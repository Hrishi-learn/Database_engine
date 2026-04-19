package command_parser;

import java.util.HashMap;
import java.util.List;

public class Command {
    CommandType type;
    String table;
    String rowId;
    HashMap<String,String>columnValueMap;
    List<String>columns;
    boolean entireRow;

    public Command(){}

    public Command(CommandType type,String table,String rowId,HashMap<String,String>columnValueMap,List<String>columns,boolean entireRow){
        this.type = type;
        this.table = table;
        this.rowId = rowId;
        this.columnValueMap = columnValueMap;
        this.columns = columns;
        this.entireRow = entireRow;
    }

    public String getRowId() {
        return rowId;
    }

    public String getTable() {
        return table;
    }

    public boolean isEntireRow() {
        return entireRow;
    }

    public HashMap<String, String> getColumnValueMap() {
        return columnValueMap;
    }

    public CommandType getType() {
        return type;
    }

    public List<String> getColumns() {
        return columns;
    }
}
