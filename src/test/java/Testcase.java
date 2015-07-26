package com.HiveUDF.Containsstring;

import java.util.ArrayList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject; 
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject; 


public class Testcase {
		  
		  @Test
		  public void ContainsStringTestReturnsCorrectValues() throws HiveException {
		   
		    ContainsString example = new ContainsString();
		    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		    ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
		    JavaBooleanObjectInspector resultInspector = (JavaBooleanObjectInspector) example.initialize(new ObjectInspector[]{listOI, stringOI});
		    
		    // Instantiate UDF arguments
		    ArrayList<String> list = new ArrayList<String>();
		    list.add("India");
		    list.add("Poland");
		    list.add("Australia");
		    
		    // test 		    
		    // the value exists
		    Object result = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("Australia")});
		    Assert.assertEquals(true, resultInspector.get(result));
		    
		    // the value doesn't exist
		    Object result2 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("China")});
		    Assert.assertEquals(false, resultInspector.get(result2));
		    
		    // arguments are null
		    Object result3 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(null), new DeferredJavaObject(null)});
		    Assert.assertNull(result3);
		  }
		
}
