
/**
 * partitionV2 for NewRowColStoreEng
 *
 * This class is different from "partition" class in that it store all data types in one array of the type long.
 *  String: store position (long) to the stringbuffer into the main array table
 *  Long: store as long
 *  Boolean: Store as long - True as 1 and false as 0
 *  Double: same as the SCUBA paper
 *
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


public class partitionV2{
    protected long UNDEFINED= -1111111; //represent NULL
    protected String separator=":"; // special character to seprate key and type, use : for now
    
    //---------------- Meta-data that stores keys and object ids ------------------------//
    int numObject;
    int[] objectIds; //list of object ids stored in this partition
    String[] keys;
    
    //----------- Actual tables that store the data in this partition in memory -----------//
    long[] data;
    
    //partition constructor, given a partition definition and max object size, initialize tables and meta data
    public partitionV2(String partition, int max_size){
        //max_size is the maximum number of Objects, NOT max bytes
        keys = partition.split("\\s+"); //separted by any white space
        data = new long[max_size];
        objectIds= new int[max_size/keys.length];
        Arrays.fill(objectIds, -1);
        numObject = 0;
    }
    
    public void insertObject(int objid, String key, long value){
        int latestId = objectIds[numObject];
        
        if(objid != latestId){
            for(int i = 0; i < keys.length; i++){
                if(keys[i].equals(key)){
                    data[(keys.length * numObject + i)] = value;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
            objectIds[numObject] = objid;
        }else{
            for(int i = 0; i < keys.length; i++){
                if(keys[i].equals(key)){
                    data[(keys.length * numObject + i)] = value;
                }
            }
        }
        return;
    }
    public void doneInsertingObject(int objid){
        if(objectIds[numObject] == objid){//this objid was just been added to this partition
            numObject++;
        }
    }
    
    /*
    public void insertObject(int objid, Hashtable<String, Long> tempLongs, Hashtable<String, Double> tempDoubles, Hashtable<String, String> tempStrings, Hashtable<String, String> tempBool, ByteBuffer stringBuffer){
        if(keys == null){
            System.out.println("please contruct this partition first.");
            return;
        }
        if(numObject == objectIds.length){
            System.out.println("Full. No inserts any more!");
            return;
        }
        
        boolean emptyRow = true;
        
        //System.out.print(objid + " ");
        for(int i = 0; i < keys.length; i++){
            String[] parts = keys[i].split(separator);
            if(parts[1].equals("STRING")){
                if(tempStrings.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = stringBuffer.position();
                    stringBuffer.putInt(tempStrings.get(keys[i]).length());
                    stringBuffer.put(tempStrings.get(keys[i]).getBytes());
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("LONG")){
                if(tempLongs.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = tempLongs.get(keys[i]);
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("DOUBLE")){
                if(tempDoubles.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = (long)( tempDoubles.get(keys[i]) * 100000);
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("BOOL")){
                if(tempBool.get(keys[i]) != null){
                    if(tempBool.get(keys[i]).equals("TRUE")){
                        data[(keys.length * numObject + i)] = (long)1;
                    }else if(tempBool.get(keys[i]).equals("FALSE")){
                        data[(keys.length * numObject + i)] = (long)0;
                    }
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
        }
        
        if(emptyRow == false){
            objectIds[numObject] = objid;
            numObject++;
        }
        return;
    }*/
    
    public void getObjectFromPartition(int objid){
        for(int i = 0; i < numObject; i++){
            if(objectIds[i] > objid){
                return;
            }
            if(objectIds[i] == objid){
                //TODO:
            }
        }
        return;
    }
    
    public void select(byte[][] columns, ResultSetV2 result){
        boolean[] access = new boolean[keys.length];
        boolean haveFields = false;
        for(int i = 0; i < keys.length; i++){
            access[i] = false;
            for(int j = 0; j < columns.length; j++){
                String key = keys[i].split(separator)[0];
                if(key.equals( new String(columns[j]) )){
                    access[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keys.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < access.length; j++){
                if(access[j]){
                    long longnum = data[i*numOfFields + j];
                    String key = keys[j];
                    if(longnum != UNDEFINED)
                        result.addLong(key, oid, longnum);
                }
            }
        }
        return;
    }
    
    /*public void selectConditionSingle(List<Integer> oidList, ResultSetV2 result){
        int index = 0;
        int targetId = oidList.get(index);
        String key = keys[0];
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
    
    public void selectConditionSingle(int[] oids, ResultSetV2 result){
        int index = 0;
        int targetId = oids[index];
        String key = keys[0];
        
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
    
    public void selectCondition(int[] oids, byte[][] columns, ResultSetV2 result){
        if(keys.length == 1){
            selectConditionSingle(oids, result);
            return;
        }
        //long start = System.currentTimeMillis();

        boolean[] access = new boolean[keys.length];
        if(columns[0][0]==(byte) '*' ){
            for(int i = 0; i < access.length; i++)
                access[i] = true;
        }else{
            for(int i = 0; i < keys.length; i++){
                access[i] = false;
                for(int j = 0; j < columns.length; j++){
                    String key = keys[i].split(separator)[0];
                    if(key.equals( new String(columns[j]) )){
                        access[i] = true;
                        break;
                    }
                }
            }
        }
        
        
        int numOfFields = keys.length;
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
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        long num = data[i*numOfFields + j];
                        String key = keys[j];
                        if(num != UNDEFINED)
                            result.addLong(key, oid, num);
                    }
                }
            }
            
        }
        //long end = System.currentTimeMillis();
        //System.out.println(end -start);
        
        return;
    }

    /*public void selectCondition(List<Integer> oidList, byte[][] columns, ResultSetV2 result){
        if(keys.length == 1){
            selectConditionSingle(oidList, result);
            return;
        }
        
        boolean[] access = new boolean[keys.length];
        
        if(columns[0][0]==(byte) '*' ){
            for(int i = 0; i < access.length; i++)
                access[i] = true;
        }else{
            for(int i = 0; i < keys.length; i++){
                access[i] = false;
                for(int j = 0; j < columns.length; j++){
                    String key = keys[i].split(separator)[0];
                    if(key.equals( new String(columns[j]) )){
                        access[i] = true;
                        break;
                    }
                }
            }
        }
        int numOfFields = keys.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        long num = data[i*numOfFields + j];
                        String key = keys[j];
                        if(num != UNDEFINED)
                            result.addLong(key, oid, num);
                    }
                }
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
    
    //this is the partition with where field
    public List<Integer> selectRangeWherePar(byte[][] selectCols, byte[] whereCol, long value1, long value2, ResultSetV2 result){
        List<Integer> oidList = new ArrayList<Integer>();
        
        //assuming the where col is in type LONG
        boolean[] access = new boolean[keys.length];
        int whereIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(whereCol) )) whereIndex = i;
        
            access[i] = false;
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                access[i] = true;
                continue;
            }
            
            for(int j = 0; j < selectCols.length; j++){
                if(key.equals( new String(selectCols[j]) )){
                    access[i] = true;
                    break;
                }
            }
        }
        
        
        if(keys.length == 1){
            for(int i = 0; i < numObject; i++){
                long value = data[i];
                if( (value >= value1) && (value <= value2)){
                    //this object meets condition
                    int oid = objectIds[i];
                    oidList.add(oid);
                    
                    if(access[0]){
                        result.addLong(keys[0], oid, value);
                    }
                }
            }
        }else{
        int numOfFields = keys.length;
        for(int i = 0; i < numObject; i++){
            //first check if this object meet the where condition
            long value = data[i*numOfFields + whereIndex];
            if( (value >= value1) && (value <= value2)){ //this object meets condition
                int oid=objectIds[i];
                oidList.add(oid);
                //select fields in the select clause
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        String key = keys[j];
                        if(j == whereIndex){
                            result.addLong(key, oid, value);
                            continue;
                        }
                        long longnum = data[i*numOfFields + j];
                        if(longnum != UNDEFINED)
                            result.addLong(key, oid, longnum);
                    }
                }
            }
        }
        }
        return oidList;
    }
    
    //this is the partition with where field
    public List<Integer> selectWhereSingleWherePar(byte[][] selectCols, byte[] whereCol, String relation, byte[] value, ResultSetV2 result, ByteBuffer stringReadBuf){
        List<Integer> oidList = new ArrayList<Integer>();
        
        //assuming the where col is in type STRING
        boolean[] access = new boolean[keys.length];
        int whereIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(whereCol) )) whereIndex = i;
            
            access[i] = false;
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                access[i] = true;
                continue;
            }
            
            for(int j = 0; j < selectCols.length; j++){
                if(key.equals( new String(selectCols[j]) )){
                    access[i] = true;
                    break;
                }
            }
        }
        
        int numOfFields = keys.length;
        boolean conditionFlag = false;
        byte[] valstr;
        for(int i = 0; i < numObject; i++){
            //first check if this object meet the where condition
            long pos = data[i*numOfFields + whereIndex];
            if(pos == UNDEFINED){
                continue;
            }
            conditionFlag = true;
            stringReadBuf.position((int)pos);
            int len = stringReadBuf.getInt();
            if (len != value.length){
            	conditionFlag = false;
            	continue;
            }
            
            valstr = new byte[len];
			stringReadBuf.get(valstr);
            for(int k = 0; k < len; k++){
                if(valstr[k] != value[k]){
                    // not equals
                    conditionFlag = false;
                    break;
                }   
            }

            if(conditionFlag){ //this object meets condition
                int oid=objectIds[i];
                oidList.add(oid);
                //select fields in the select clause
                for(int j = 0; j < access.length; j++){
                    
                    if(access[j]){
                        String key = keys[j];
                        if(j == whereIndex){
                            result.addLong(key, oid, pos);
                            continue;
                        }
                        long longnum = data[i*numOfFields + j];
                        if(longnum != UNDEFINED)
                            result.addLong(key, oid, longnum);
                    }
                }
            }
        }
        return oidList;
    }
    
    //this is the partition with where field, for selectWhereAny, just check conditions and return the oidlist that meet the condition, but not selecting anything at this point
    public List<Integer> selectWhereAnyCheckCondition(byte[] whereCol, String relation, byte[] value, ByteBuffer stringReadBuf, List<Integer> oidList){
        boolean[] whereAccess = new boolean[keys.length];
        
        for(int i = 0; i < keys.length; i++){
            whereAccess[i] = false;
            String key = keys[i].split(separator)[0];
            String arrayKey = key.split("\\[")[0];
            if(arrayKey.equals( new String(whereCol) )){
                whereAccess[i] = true;
            }
        }
        
        int numOfFields = keys.length;
        boolean conditionFlag = false;
        byte[] valstr;
        for(int i = 0; i < numObject; i++){
            for(int j = 0; j < whereAccess.length; j++){
                if(whereAccess[j]){
                    long pos = data[i*numOfFields + j];
                    if(pos == UNDEFINED){
                        continue;
                    }
                    conditionFlag = true;
                    stringReadBuf.position((int)pos);
                    int len = stringReadBuf.getInt();
                    if (len != value.length){
                        conditionFlag = false;
                        continue;
                    }
                    
                    valstr = new byte[len];
                    stringReadBuf.get(valstr);
                    for(int k = 0; k < len; k++){
                        if(valstr[k] != value[k]){
                            // not equals
                            conditionFlag = false;
                            break;
                        }
                    }
                    if(conditionFlag){ //this object meets condition
                        int oid=objectIds[i];
                        oidList.add(oid);
                        break;
                    }
                }
            }
        }
        return oidList;
    }
    
    //this is the partition with where field
    public List<Integer> aggregateRangeGroupByCheckCondition(byte[] whereCol, long value1, long value2){
        List<Integer> oidList = new ArrayList<Integer>();
        
        int whereIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(whereCol) )) whereIndex = i;
        }
        
        int numOfFields = keys.length;
        for(int i = 0; i < numObject; i++){
            //first check if this object meet the where condition
            long value = data[i*numOfFields + whereIndex];
            if( (value >= value1) && (value <= value2)){ //this object meets condition
                int oid=objectIds[i];
                oidList.add(oid);
            }
        }
        return oidList;
    }
    
    //If this is the partition with the group field
    public Hashtable<String, Integer> aggregateGroupBy(byte[] gColumn, int[] oids, String type, ByteBuffer stringReadBuf){
        int groupIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(gColumn) )) groupIndex = i;
        }

        if(groupIndex == -1){
            System.out.println("Cannot find the GROUP BY field");
            return null;
        }
        
        Hashtable<String, Integer> resultSet= new Hashtable<String, Integer>();
        
        int numOfFields = keys.length;
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
                long num = data[i*numOfFields + groupIndex];
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
    /*
    public HashMap<String, Integer> scanLeft(int[] oids, byte[] jCol, HashMap<String, Integer> hash, String type, ByteBuffer stringReadBuf){
        
        int jIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(jCol) )) jIndex = i;
        }
        
        if(jIndex == -1){
            System.out.println("Cannot find the JOIN condition field");
            return null;
        }
        int numOfFields = keys.length;
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
                long num = data[i*numOfFields + groupIndex];
                String key;
                if(type.equals("LONG")){
                    key = new String(Long.toString(num));
                }else if(type.equals("DOUBLE")){
                    key = String.valueOf((double) num/100000);
                }else if(type.equals("STRING")){
                    stringReadBuf.position(num);
                    long len = stringReadBuf.getInt();
                    byte[] valstr = new byte[len];
                    stringReadBuf.get(valstr);
                    key = new String(bytes, "UTF-8");
                }else if(type.equals("BOOL")){
                    if(num == 1)
                        key = "TRUE";
                    else
                        key = "FALSE";
                }
                hash.put(key, oid);
            }
        }
        return hash;
    }
    
    public HashMap<String, Integer[]> scanRight(int[] oids, byte[] jCol, HashMap<String, Integer> hash, String type, ByteBuffer stringReadBuf){
        
        int jIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(jCol) )) jIndex = i;
        }
        
        if(jIndex == -1){
            System.out.println("Cannot find the JOIN condition field");
            return null;
        }
        int numOfFields = keys.length;
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
                long num = data[i*numOfFields + groupIndex];
                String key;
                if(type.equals("LONG")){
                    key = new String(Long.toString(num));
                }else if(type.equals("DOUBLE")){
                    key = String.valueOf((double) num/100000);
                }else if(type.equals("STRING")){
                    stringReadBuf.position(num);
                    long len = stringReadBuf.getInt();
                    byte[] valstr = new byte[len];
                    stringReadBuf.get(valstr);
                    key = new String(bytes, "UTF-8");
                }else if(type.equals("BOOL")){
                    if(num == 1)
                        key = "TRUE";
                    else
                        key = "FALSE";
                }
                hash.put(key, oid);
            }
        }
        return hash;
    }
    */
    public int getNullCount(){
        int count = 0;
        for(int i = 0; i < numObject; i++){
            for(int j = 0; j < keys.length; j++){
                if(data[i*keys.length+j] == UNDEFINED){
                    count++;
                }
            }
        }
        return count;
    }
    
}
