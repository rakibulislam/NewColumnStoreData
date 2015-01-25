
/**
 * Save JSON file into memory based column store 
 * Every JSON field is saved into a column, represented by a buffer    
 * 
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Jin Chen, Alan Lu
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

public class NewColStoreEng extends StoreEngine  {

    /*public class Column{
        int numObject;
        int[] objectIds;
        String key;
        String type;
        
        //----------- Actual tables that store the data in this partition in memory -----------//
        long[] longValues;
        double[] doubleValues;
        byte[] booleanValues;
        long[] stringValues;    //stores location to the stringBuffer where the actual string is store
        
        public Column(String columnKey, int max_size){
            //max_size is the maximum number of Objects, NOT max bytes
            
            //assuming columnKey is in the format of "KEY:TYPE"
            String[] parts = columnKey.split(separator);
            
            if(parts[1].equals("STRING")){
                stringValues = new long[max_size];
                type = "STRING";
            } else if(parts[1].equals("LONG")){
                longValues = new long[max_size];
                type = "LONG";
            } else if(parts[1].equals("DOUBLE")){
                doubleValues = new double[max_size];
                type = "DOUBLE";
            } else if(parts[1].equals("BOOL")){
                booleanValues = new byte[max_size];
                type = "BOOL";
            }
            
            key = new String(columnKey);
            objectIds= new int[max_size];
            numObject = 0;
        }
        
        public void saveStrValue(int objid, long pos){
            stringValues[numObject] = pos;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveLongValue(int objid, long longnum){
            longValues[numObject] = longnum;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveDoubleValue(int objid, double num){
            doubleValues[numObject] = num;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveBoolValue(int objid, byte bool){
            booleanValues[numObject] = bool;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void selectString(ResultSet result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long pos = stringValues[i];
                //if(pos != UNDEFINED)
                    result.addString(key, oid, pos);
            }
            return;
        }
        
        public void selectLong(ResultSet result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long longnum = longValues[i];
                //if(longnum != UNDEFINED)
                    result.addLong(key, oid, longnum);
            }
            return;
        }
        
        public void selectDouble(ResultSet result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                double num = doubleValues[i];
                //if(num != UNDEFINED)
                    result.addDouble(key, oid, num);
            }
            return;
        }
        
        public void selectBool(ResultSet result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                byte bool = booleanValues[i];
                //if(bool != UndefByte[0])
                    result.addBool(key, oid, bool);
            }
            return;
        }
        
        //given a list of oid, select all objects with oids in the list, Assuming this is only called if this is a column with type string
        public void selectConditionString(List<Integer> oidList, ResultSet result){
            
            int index = 0;
            int targetId = oidList.get(index);
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                
                if(oid == targetId){
                    long pos = stringValues[i];
                    result.addString(key,oid,pos);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
            return;
        }
        
        //given a list of oid, select all objects with oids in the list, Assuming this is only called if this is a column with type long
        public void selectConditionLong(List<Integer> oidList, ResultSet result){
            
            int index = 0;
            int targetId = oidList.get(index);
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                
                if(oid == targetId){
                    long longnum = longValues[i];
                    result.addLong(key,oid,longnum);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
            return;
        }
        
        //given a list of oid, select all objects with oids in the list, Assuming this is only called if this is a column with type double
        public void selectConditionDouble(List<Integer> oidList, ResultSet result){
            
            int index = 0;
            int targetId = oidList.get(index);
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                
                if(oid == targetId){
                    double num = doubleValues[i];
                    result.addDouble(key,oid,num);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
            return;
        }
        
        //given a list of oid, select all objects with oids in the list, Assuming this is only called if this is a column with type bool
        public void selectConditionBool(List<Integer> oidList, ResultSet result){
            
            int index = 0;
            int targetId = oidList.get(index);
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                
                if(oid == targetId){
                    byte bool = booleanValues[i];
                    result.addBool(key,oid,bool);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
            return;
        }
        
        //this is the column with where field, assume this is only called if this column is long
        public List<Integer> selectRangeWhereCol(byte[][] selectCols, long value1, long value2, ResultSet result){
            List<Integer> oidList = new ArrayList<Integer>();
            
            boolean alsoSelect = false;
            //This is the WHERE col, but also check if we should also select this col
            for(int i = 0; i < selectCols.length; i++){
                if(key.equals( new String(selectCols[i]) + separator + "LONG" )){
                    alsoSelect = true;
                    break;
                }
            }
            
            for(int i = 0; i < numObject; i++){
                long value = longValues[i];
                if( (value >= value1) && (value <= value2)){
                    //this object meets condition
                    int oid = objectIds[i];
                    oidList.add(oid);
                    
                    if(alsoSelect){
                        result.addLong(key, oid, value);
                    }
                }
            }
            return oidList;
        }
        
        public void selectCondition(List<Integer> oidList, ResultSet result){
            if(type.equals("STRING")){
                selectConditionString(oidList, result);
            }else if(type.equals("LONG")){
                selectConditionLong(oidList, result);
            }else if(type.equals("DOUBLE")){
                selectConditionDouble(oidList, result);
            }else if(type.equals("BOOL")){
                selectConditionBool(oidList, result);
            }
            
            return;
        }
        
    } // end of the Column Class
    */
    
    /* This class is different from "Column" class in that it store all data types in one array of the type long.
    *  String: store position (long) to the stringbuffer into the main array table
    *  Long: store as long
    *  Boolean: Store as long - True as 1 and false as 0
    *  Double: same as the SCUBA paper
    */
    public class ColumnV2{
        int numObject;
        int[] objectIds;
        String key;
        String type;
        
        //----------- Actual tables that store the data in this partition in memory -----------//
        long[] data;
        
        public ColumnV2(String columnKey, int max_size){
            //max_size is the maximum number of Objects, NOT max bytes
            
            //assuming columnKey is in the format of "KEY:TYPE"
            String[] parts = columnKey.split(separator);
            
            if(parts[1].equals("STRING")){
                type = "STRING";
            } else if(parts[1].equals("LONG")){
                type = "LONG";
            } else if(parts[1].equals("DOUBLE")){
                type = "DOUBLE";
            } else if(parts[1].equals("BOOL")){
                type = "BOOL";
            }
            
            data = new long[max_size];
            key = new String(columnKey);
            objectIds= new int[max_size];
            numObject = 0;
        }
        
        public void saveStrValue(int objid, long pos){
            data[numObject] = pos;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveLongValue(int objid, long longnum){
            data[numObject] = longnum;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveDoubleValue(int objid, double num){
            data[numObject] = (long) num * 100000;
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        public void saveBoolValue(int objid, byte bool){
            if(bool == (byte)1){
                data[numObject] = (long) 1;
            } else if(bool == (byte)0){
                data[numObject] = (long) 0;
            }
            objectIds[numObject] = objid;
            numObject++;
            return;
        }
        
        /*public void selectString(ResultSetV2 result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long pos = data[i];
                result.addLong(key, oid, pos);
            }
            return;
        }*/
        
        public void selectLong(ResultSetV2 result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long longnum = data[i];
                result.addLong(key, oid, longnum);
            }
            return;
        }
        
        /*public void selectDouble(ResultSetV2 result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long num = data[i];
                result.addLong(key, oid, num);
            }
            return;
        }*/
        
        /*public void selectBool(ResultSetV2 result){
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                long boolVal = data[i];
                result.addLong(key, oid, boolVal);
            }
            return;
        }*/
        
        //this is the column with where field, assume this is only called if this column is long
        public List<Integer> selectRangeWhereCol(byte[][] selectCols, long value1, long value2, ResultSetV2 result){
            List<Integer> oidList = new ArrayList<Integer>();
            
            boolean alsoSelect = false;
            //This is the WHERE col, but also check if we should also select this col
            for(int i = 0; i < selectCols.length; i++){
                if(key.equals( new String(selectCols[i]) + separator + "LONG" )){
                    alsoSelect = true;
                    break;
                }
            }
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                alsoSelect = true;
            }
            
            for(int i = 0; i < numObject; i++){
                long value = data[i];
                if( (value >= value1) && (value <= value2)){
                    //this object meets condition
                    int oid = objectIds[i];
                    oidList.add(oid);
                    
                    if(alsoSelect){
                        result.addLong(key, oid, value);
                    }
                }
            }
            return oidList;
        }
        
        //this is the column with where field, assume this is only called if this column is string
        public List<Integer> selectWhereSingleWhereCol(byte[][] selectCols, String relation, byte[] conditionStr, ResultSetV2 result, ByteBuffer stringReadBuf){
            List<Integer> oidList = new ArrayList<Integer>();
            
            boolean alsoSelect = false;
            //This is the WHERE col, but also check if we should also select this col
            for(int i = 0; i < selectCols.length; i++){
                if(key.equals( new String(selectCols[i]) + separator + "STRING" )){
                    alsoSelect = true;
                    break;
                }
            }
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                alsoSelect = true;
            }
            
            boolean conditionFlag = false;
            byte[] valstr;
            for(int i = 0; i < numObject; i++){
                long value = data[i];
                conditionFlag = true;
                stringReadBuf.position((int)value);
                int len = stringReadBuf.getInt();
                if (len != conditionStr.length){
                    conditionFlag = false;
                    continue;
                }
                
                valstr = new byte[len];
                stringReadBuf.get(valstr);
                for(int k = 0; k < len; k++){
                    if(valstr[k] != conditionStr[k]){
                        // not equals
                        conditionFlag = false;
                        break;
                    }   
                }

                if(conditionFlag){
                    //this object meets condition
                    int oid = objectIds[i];
                    oidList.add(oid);
                    
                    if(alsoSelect){
                        result.addLong(key, oid, value);
                    }
                }
            }
            return oidList;
        }
        
        //this is the col with where field for selectWhereAny, just check conditions and return the oidlist that meet the condition, but not selecting anything at this point
        public List<Integer> selectWhereAnyCheckCondition(byte[] whereCol, String relation, byte[] conditionStr, ByteBuffer stringReadBuf, List<Integer> oidList){
            
            boolean conditionFlag = false;
            byte[] valstr;
            for(int i = 0; i < numObject; i++){
                long value = data[i];
                conditionFlag = true;
                stringReadBuf.position((int)value);
                int len = stringReadBuf.getInt();
                if (len != conditionStr.length){
                    conditionFlag = false;
                    continue;
                }
                
                valstr = new byte[len];
                stringReadBuf.get(valstr);
                for(int k = 0; k < len; k++){
                    if(valstr[k] != conditionStr[k]){
                        // not equals
                        conditionFlag = false;
                        break;
                    }
                }
                
                if(conditionFlag){
                    int oid=objectIds[i];
                    oidList.add(oid);
                }
            }

            return oidList;
        }

        //this is the col with where field
        public List<Integer> aggregateRangeGroupByCheckCondition(long value1, long value2){
            List<Integer> oidList = new ArrayList<Integer>();
            
            //int numOfFields = keys.length;
            for(int i = 0; i < numObject; i++){
                //first check if this object meet the where condition
                long value = data[i];
                if( (value >= value1) && (value <= value2)){ //this object meets condition
                    int oid=objectIds[i];
                    oidList.add(oid);
                }
            }
            return oidList;
        }
        
        //If this is the col with the group field
        public Hashtable<String, Integer> aggregateGroupBy(int[] oids, String type, ByteBuffer stringReadBuf){
            
            Hashtable<String, Integer> resultSet= new Hashtable<String, Integer>();
            
            int index = 0;
            int targetId = oids[index];
            
            for(int i = 0; i < numObject; i++){
                int oid=objectIds[i];
                if(oid > targetId){
                    while(oid > targetId){
                        index++;
                        if(index < oids.length)
                            targetId = oids[index];
                        else
                            break;
                    }
                }
                
                if(oid == targetId){
                    long num = data[i];
                    String rKey = "";
                    if(type.equals("LONG")){
                        rKey = new String(Long.toString(num));
                    }else if(type.equals("DOUBLE")){
                        rKey = String.valueOf((double) num/100000);
                    }else if(type.equals("STRING")){
                        stringReadBuf.position((int)num);
                        int len = stringReadBuf.getInt();
                        byte[] valstr = new byte[len];
                        stringReadBuf.get(valstr);
                        rKey = new String(valstr);
                    }else if(type.equals("BOOL")){
                        if(num == 1)
                            rKey = "TRUE";
                        else
                            rKey = "FALSE";
                    }
                    
                    Integer value = resultSet.get(rKey);
                    if(value!=null){
                        resultSet.put(rKey, value+1);
                    }else{
                        resultSet.put(rKey,1);
                    }
                }
            }
            return resultSet;
        }

        
        //given a list of oid, select all objects with oids in the list
        /*public void selectCondition(List<Integer> oidList, ResultSetV2 result){
            int index = 0;
            int targetId = oidList.get(index);
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                
                if(oid == targetId){
                    long longnum = data[i];
                    result.addLong(key,oid,longnum);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
            return;
        }*/
        
        public void selectCondition(int[] oids, ResultSetV2 result){
            int index = 0;
            int targetId = oids[index];
            
            for(int i = 0; i < numObject; i++){
                int oid = objectIds[i];
                if(oid > targetId){
                    while(oid > targetId){
                        index++;
                        if(index < oids.length)
                            targetId = oids[index];
                        else
                            break;
                    }
                }
                
                if(oid == targetId){
                    long longnum = data[i];
                    result.addLong(key,oid,longnum);
                }
            }
            return;
        }
        
    } // end of the ColumnV2 Class

    
	private ByteBuffer stringBuffer;
    ResultSetV2 result;
    Hashtable<String, ColumnV2> cols;
    
    public NewColStoreEng(int memory_size_in_bytes)
    {
        //call parent constructor
        super(memory_size_in_bytes);
        /* create colum Buffer hash table */ 
        //colBufs = new Hashtable<String, ByteBuffer>();
        
        cols = new Hashtable<String, ColumnV2>();
		stringBuffer = ByteBuffer.allocateDirect(max_buf_size*100);
        stringBuffer.position(0);
		result = new ResultSetV2(100*1000*1000);
    }
    
    /**
     * Navigate the json object and parse it and save it into storage
     */
    public void insertObject(int objid, JsonValue tree, String key){
        
        switch(tree.getValueType()){
            case OBJECT:
                JsonObject object = (JsonObject) tree;
                for(String name: object.keySet()){
                    if(key!=null)
                        insertObject(objid,object.get(name),key+"."+name);
                    else
                        insertObject(objid,object.get(name),name);
                }
                break;
            case ARRAY:
                JsonArray array = (JsonArray) tree;
                int index =0;
                for (JsonValue val : array){
                    insertObject(objid,val,key+"["+index+"]");
                    index += 1;
                }
                break;
            case STRING:
                JsonString st = (JsonString) tree;
                saveStrValue(objid,key,st.getString());
                break;
            case NUMBER:
                JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
                break;
            case TRUE:
            case FALSE:
                saveBoolValue(objid,key,tree.getValueType().toString());
                break;
            case NULL:
                // we didn't save null value
                break;
        }
    }

    protected int saveStrValue(int objid, String key, String value)
    {
        //later -- use dictionary to compress
        // save to str
        String bufkey = key+separator+"STRING";
        
        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size));
        }
        
        long position = stringBuffer.position();
        cols.get(bufkey).saveStrValue(objid, position);
        
        ////rakib change here to compress before putting
		System.out.println ("RAKIB COMPRESS HERE....");
        stringBuffer.putInt(value.length());
        stringBuffer.put(value.getBytes());
        
        return 1;
    }

    protected int saveLongValue(int objid, String key, long num)
    {
        String bufkey = key+separator+"LONG";
        
        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size));
        }
        
        cols.get(bufkey).saveLongValue(objid, num);
        
        return 1;
    } 

    protected int saveDoubleValue(int objid, String key, double num)
    {
        String bufkey = key+separator+"DOUBLE";
        
        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size));
        }

        cols.get(bufkey).saveDoubleValue(objid, num);
        
        return 1;
    } 

    protected int saveBoolValue(int objid, String key, String value)
    {
        String bufkey = key+separator+"BOOL";
        
        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size));
        }
        
        if(value.equals("TRUE")==true){
            cols.get(bufkey).saveBoolValue(objid, (byte)1);
        }else if(value.equals("FALSE")==true){
            cols.get(bufkey).saveBoolValue(objid, (byte)0);
        }else{
            System.out.println("Error: unknow value "+value);
        }
        
        return 1;
    }
    

    /** TODO: finish this
     */

    public int getObject(int targetId){
        return 0;
    }
    
    public void select(byte[][] columns){
        result.clearResultSet();
        
        if(columns[0][0]==(byte) '*' ){//select all fields
            for(String col: cols.keySet()){
                cols.get(col).selectLong(result);
            }
            return;
        }
        
        for(int i = 0; i< columns.length; i++){
            // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type
			String selectKey = new String(columns[i]) + separator + "STRING";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectLong(result);
                //cols.get(selectKey).selectString(result);
            
            selectKey = new String(columns[i]) + separator + "LONG";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectLong(result);
            
            selectKey = new String(columns[i]) + separator + "DOUBLE";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectLong(result);
                //cols.get(selectKey).selectDouble(result);
            
            selectKey = new String(columns[i]) + separator + "BOOL";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectLong(result);
                //cols.get(selectKey).selectBool(result);
        } //end for
        return;
    }
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2)
    {
        result.clearResultSet();

        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = cols.get( where ).selectRangeWhereCol(selectCols, value1, value2, result);
        
        if(oidList.size() > 0){
            Hashtable<String, String> colsToSelect = new Hashtable<String, String>();

            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String col: cols.keySet()){
                    colsToSelect.put(col, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(cols.get(selectField + separator + "LONG") != null)
                        colsToSelect.put(selectField + separator + "LONG", "");
                    
                    if(cols.get(selectField + separator + "DOUBLE") != null)
                        colsToSelect.put(selectField + separator + "DOUBLE", "");
                    
                    if(cols.get(selectField + separator + "STRING") != null)
                        colsToSelect.put(selectField + separator + "STRING", "");
                    
                    if(cols.get(selectField + separator + "BOOL") != null)
                        colsToSelect.put(selectField + separator + "BOOL", "");
                }
            }
            
            //the column that contains the WHERE has been already selected as we checked condition
            colsToSelect.remove( where );

            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String col: colsToSelect.keySet()){
                //cols.get(col).selectCondition(oidList, result);
                cols.get(col).selectCondition(oids, result);
            }
        }
        
        result.shuffle();
        
        return;
    }
    /*
     select x,y,z where a = ANY xx
     * xx is a set, single column and single relation parsing
     * Method:  there are multiple columns for this set xx,
     *         scan all of where columns and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     *        assume string type for now
     */
    public HashMap<Integer, HashMap<String, String>>  selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();
        
        Hashtable<String, String> whereCols = new Hashtable<String, String>();
        List<Integer> oidList = new ArrayList<Integer>();
        
        // scan each related column xx[0],xx[1],...
        int count = 0;
        while(true){
            String whereKey = new String(whereCol)+"["+String.valueOf(count)+"]"+separator+"STRING";
            if(cols.get(whereKey) == null){
                break;
            }
            count++;
            whereCols.put(whereKey,"");
        }
        
        for(String col: whereCols.keySet()){
            oidList = cols.get( col ).selectWhereAnyCheckCondition(whereCol, relation, value, stringBuffer, oidList);
        }
        
        Collections.sort(oidList);
        
        if(oidList.size() > 0){
            Hashtable<String, String> colsToSelect = new Hashtable<String, String>();
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String col: cols.keySet()){
                    colsToSelect.put(col, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(cols.get(selectField + separator + "LONG") != null)
                        colsToSelect.put(selectField + separator + "LONG", "");
                    
                    if(cols.get(selectField + separator + "DOUBLE") != null)
                        colsToSelect.put(selectField + separator + "DOUBLE", "");
                    
                    if(cols.get(selectField + separator + "STRING") != null)
                        colsToSelect.put(selectField + separator + "STRING", "");
                    
                    if(cols.get(selectField + separator + "BOOL") != null)
                        colsToSelect.put(selectField + separator + "BOOL", "");
                }
            }
            
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String col: colsToSelect.keySet()){
                //cols.get(col).selectCondition(oidList, result);
                cols.get(col).selectCondition(oids, result);
            }
        }

        return null;
    }
    

    /*
     * select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing 
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();
        
        String where = (new String(whereCol)) + separator + "STRING";
        List<Integer> oidList = cols.get( where ).selectWhereSingleWhereCol(selectCols, relation, value, result, stringBuffer);
        
        if(oidList.size() > 0){
            Hashtable<String, String> colsToSelect = new Hashtable<String, String>();
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String col: cols.keySet()){
                    colsToSelect.put(col, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(cols.get(selectField + separator + "LONG") != null)
                        colsToSelect.put(selectField + separator + "LONG", "");
                    
                    if(cols.get(selectField + separator + "DOUBLE") != null)
                        colsToSelect.put(selectField + separator + "DOUBLE", "");
                    
                    if(cols.get(selectField + separator + "STRING") != null)
                        colsToSelect.put(selectField + separator + "STRING", "");
                    
                    if(cols.get(selectField + separator + "BOOL") != null)
                        colsToSelect.put(selectField + separator + "BOOL", "");
                }
            }
            
            //the column that contains the WHERE has been already selected as we checked condition
            colsToSelect.remove( where );
            
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String col: colsToSelect.keySet()){
                //cols.get(col).selectCondition(oidList, result);
                cols.get(col).selectCondition(oids, result);
            }
        }

        return null;
    }
    
    /*
     * select count(*) where a between value1 and value2 GROUP By K
     * Note: seems selectCols are not needed
     */
    public Hashtable<String, Integer> aggregateRangeGroupBy(byte[][] selectCols, byte[] whereCol, long value1, long value2, byte[] gColumn)
    {
        //result.clearResultSet();
        
        //First check condition and get a list of oids
        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = cols.get( where ).aggregateRangeGroupByCheckCondition(value1, value2);
        Hashtable<String, Integer> resultSet = new Hashtable<String, Integer>();
        //Then do the count based on the gColumn
        if(oidList.size() > 0){
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            String gField = new String(gColumn);
            if(cols.get(gField + separator + "LONG") != null)
                resultSet = cols.get(gField + separator + "LONG").aggregateGroupBy(oids, "LONG", stringBuffer);
            
            if(cols.get(gField + separator + "STRING") != null)
                resultSet = cols.get(gField + separator + "STRING").aggregateGroupBy(oids, "STRING", stringBuffer);
            
            if(cols.get(gField + separator + "DOUBLE") != null)
                resultSet = cols.get(gField + separator + "DOUBLE").aggregateGroupBy(oids, "DOUBLE", stringBuffer);
            
            if(cols.get(gField + separator + "BOOL") != null)
                resultSet = cols.get(gField + separator + "BOOL").aggregateGroupBy(oids, "BOOL", stringBuffer);
        }
		return resultSet;
    }
    
    
    public void printBriefStats(){
        result.printBriefStats();
        return;
    }

    public void printResultsToFile(String Query, String outputfile){
        HashMap<Integer, HashMap<String, Long>> resultSet = result.getAllResultsInHashMap();
        ByteBuffer readBuf = stringBuffer.asReadOnlyBuffer();
        
        try{
            String filename= outputfile;
            FileWriter fw = new FileWriter(filename,true); //the true will append the new data
            //first output the query
            fw.write(Query + ":\n");
            
            for(Integer objid: resultSet.keySet()){
                HashMap <String, Long> inner = resultSet.get(objid);
                
                fw.write("object " + objid + " {");
                
                for(String key: inner.keySet()){
                    String[] parts = key.split(separator);
                    fw.write("\"" + parts[0] + "\": ");
                    
                    if(parts[1].equals("STRING")){
                        long pos = inner.get(key).longValue();
                        readBuf.position((int)pos);
                        int len = readBuf.getInt();
                        byte [] valstr = new byte[len];
                        
                        //rakib change here to uncompress
						System.out.println ("RAKIB HERE....");
                        
                        readBuf.get(valstr);
                        fw.write("\"" + new String(valstr) + "\", ");
                    }else if(parts[1].equals("LONG")){
                        fw.write(inner.get(key).longValue() + ", ");
                    }else if(parts[1].equals("DOUBLE")){
                        double num = (double) ( inner.get(key).longValue() / 100000);
                        fw.write(num + ", ");
                    }else if(parts[1].equals("BOOL")){
                        long bool = inner.get(key).longValue();
                        if(bool == 1){
                            fw.write("true, ");
                        }else{
                            fw.write("false, ");
                        }
                    }
                }
                fw.write("}\n");
            }
            fw.close();
        }
        catch(IOException ioe){
            System.err.println("IOException: " + ioe.getMessage());
        }
    }
    
    public static void main(String[] args) throws IOException{


        /* flatten the json file */ 
        //JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
        JsonReader reader = Json.createReader(new FileReader("testjson/abc.json")); 
        JsonObject jsonob = reader.readObject();
        System.out.println(jsonob.toString());
        ColStoreEng store= new ColStoreEng(10*1000*1000);
        int objid = 1;
        store.insertObject(objid,jsonob,null);
        store.insertObject(2,jsonob,null);
        store.insertObject(3,jsonob,null);
        /* populate the table */

        System.out.println("get the result out \n");
        /* objid, keystr,valstr,valnum,valbool - 5 bytes */
        /* read it out */
        store.getObject(2);

        //aggregate scan
        String targetColumn = "A";
        long sum = store.aggregate(targetColumn.getBytes(),10);
        System.out.println("Aggregate sum results :"+sum);

    }
}
