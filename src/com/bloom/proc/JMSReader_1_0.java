 package com.bloom.proc;
 
 import com.bloom.anno.AdapterType;
 import com.bloom.anno.PropertyTemplate;
 import com.bloom.proc.events.WAEvent;
 import com.bloom.recovery.SourcePosition;
 import com.bloom.runtime.components.Flow;
 import com.bloom.security.Password;
 import com.bloom.source.lib.reader.Reader;
 import com.bloom.uuid.UUID;
 import java.util.Map;
 import java.util.TreeMap;
 
 
 
 
 
 
 
 
 
 
 
 
 
 @PropertyTemplate(name="JMSReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="UserName", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=Password.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Ctx", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Provider", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="QueueName", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="connectionfactoryname", type=String.class, required=false, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
 public class JMSReader_1_0
   extends BaseReader
 {
   public JMSReader_1_0()
   {
     this.readerType = Reader.JMS_READER;
   }
   
 
   public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow)
     throws Exception
   {
     Map<String, Object> localProp1 = new TreeMap(String.CASE_INSENSITIVE_ORDER);
     localProp1.putAll(prop1);
     if (localProp1.get("Password") != null) {
       String pp = ((Password)localProp1.get("Password")).getPlain();
       localProp1.put("Password", pp);
     }
     
     super.init(localProp1, prop2, uuid, distributionId, startPosition, sendPositions, flow);
   }
 }

