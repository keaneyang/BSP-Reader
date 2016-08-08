package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import java.io.InputStream;
import java.util.Map;

@PropertyTemplate(name="HTTPReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="ipaddress", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="portno", type=Integer.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="threadcount", type=Integer.class, required=false, defaultValue="-1"), @com.bloom.anno.PropertyTemplateProperty(name="keystoretype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="keystore", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="keystorepassword", type=com.bloom.security.Password.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="authenticateclient", type=Boolean.class, required=false, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue="")}, inputType=Event.class)
public class HTTPReader_1_0
  extends BaseReader
{
  private static final String ORIGINAL_PARSER_NAME = "parserName";
  private static final String handler = "com.bloom.proc.HTTPParser_1_0";
  private static final String SOURCE_PROCESS = "sourceProcess";
  
  public void customizePropertyMap(Map<String, Object> prop)
  {
    String parserName = (String)prop.get("handler");
    prop.put("handler", "com.bloom.proc.HTTPParser_1_0");
    prop.put("parserName", parserName);
    prop.put("sourceProcess", this);
  }
  
  public InputStream createInputStream(Map<String, Object> prop)
  {
    return null;
  }
}
