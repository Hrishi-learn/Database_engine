package exceptions;

public class TableNotFoundException extends RuntimeException{
    public TableNotFoundException(String msg){
        super(msg);
    }
}
