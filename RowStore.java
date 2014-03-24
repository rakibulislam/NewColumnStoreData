/**x
 * An row-based buffer store  
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;

public class RowStore {
	private int max_buf_size;
	private ByteBuffer buffer; //save all of the contents
	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];
    private static String [] fields;
    private static Hashtable <String, Long> tempBufLong = new Hashtable<String, Long>(); 
    private static Hashtable <String, Double> tempBufDouble = new Hashtable<String, Double>(); 
    String separator=":";
    /**
     * Creates a ring buffer
	 * initialize the buffer size 
     */
    public RowStore (int memory_size_in_bytes)
    {
		int max_buf_size = memory_size_in_bytes; 

		// create a byte buffer 
		buffer = ByteBuffer.allocateDirect(max_buf_size);
		//buffer = new byte[max_buf_size];

        UndefByte[0] = -1;
	}
    
    public int getRow(int targetId)
	{
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
		int oid = 0;
		long longnum=0;
		double doublenum=0;

		while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();
			if(oid > targetId)
				break; /* found the target row - assume monotonous increase */
			if(oid == targetId)
				System.out.print("Row "+oid);
		
			for(int i = 0; i < fields.length; i++){
				String [] parts = fields[i].split(separator);
				//String columnKey = parts[0];
	            String type = parts[1];
	            
	            if(type.equals("STRING")){
	            }else if(type.equals("LONG")){
	            	longnum = readBuf.getLong();
	            	if(oid == targetId)
	    				System.out.print(" "+longnum);
	            }else if(type.equals("DOUBLE")){
	            	doublenum = readBuf.getDouble();
	            	if(oid == targetId)
	            		System.out.print(" "+doublenum);
	            }else if(type.equals("BOOL")){
	            }else{
	                System.out.println("Error: no such type in buf "+type);
	                return 1;
	            }
			}


			if(oid == targetId)
				System.out.println("");
		} /* end while */
        return 0;
	}
    
    public void prepareRow(int objid, JsonValue tree, String key){
        switch(tree.getValueType()){
		case OBJECT:
			JsonObject object = (JsonObject) tree;
			for(String name: object.keySet()){
				if(key!=null)
					prepareRow(objid,object.get(name),key+"."+name);
				else
					prepareRow(objid,object.get(name),name);
			}
            // check the size
            if((objid % 10000) == 1) 
               System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
			break;
		case ARRAY:
			JsonArray array = (JsonArray) tree;
			int index =0; 
			for (JsonValue val : array){
				prepareRow(objid,val,key+"["+index+"]");
				index += 1;
			}
			break;
		case STRING:
			JsonString st = (JsonString) tree;
			//saveStrValue(objid,key,st.getString());
			break;
		case NUMBER:
			JsonNumber num = (JsonNumber) tree;
            if(num.isIntegral()){
            	tempBufLong.put(key, num.longValue());
            }else{
            	tempBufDouble.put(key, num.doubleValue());
            }
			break;
		case TRUE:
		case FALSE:
			//saveBoolValue(objid,key,tree.getValueType().toString());
			break;
		case NULL:
			break;
        }

	}
    
	public void insertRow(int objid){
		//leave 1K as the warning threshold - don't insert after it 
	    if(buffer.position() > buffer.capacity() - 1000){
	    	//don't insert any more -- buffer almost full
	    	System.out.println("buffer is almost full. No inserts any more!");
	    	return;
	    }
	    //first put the objid
		buffer.putInt(objid);
		
		for(int i = 0; i < fields.length; i++){
			String [] parts = fields[i].split(separator);
			String key = parts[0];
            String type = parts[1];
            
            if(type.equals("STRING")){
            }
            else if(type.equals("LONG")){
    			if(tempBufLong.get(key) != null)
    				buffer.putLong(tempBufLong.get(key));
    			else
					buffer.putLong(UNDEFINED);
            }
            else if(type.equals("DOUBLE")){
            	if(tempBufDouble.get(key) != null)
            		buffer.putDouble(tempBufDouble.get(key));
    			else
					buffer.putLong(UNDEFINED);
            }
            else if(type.equals("BOOL")){
            }
            else{
                System.out.println("Error: no such type in buf "+type);
                return;
            }
		}
	}
    
	public static void main(String[] args) throws IOException{
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde2.json")); 

		//Assuming we know all the columns and data type in advance
		fields = new String[] {"A:LONG","B:LONG","C:LONG","D:LONG","E:DOUBLE"};
		
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		RowStore store= new RowStore(100*1000*1000);
		
		//insert objects
		for(int objid=0; objid<10; objid++){
			//Clear the hashtables that temporarily stores data of an object
			tempBufLong.clear();
			tempBufDouble.clear();
			store.prepareRow(objid,jsonob,null);
			store.insertRow(objid);
		}

		System.out.println("get the result out \n");
		store.getRow(5);
	}
}
