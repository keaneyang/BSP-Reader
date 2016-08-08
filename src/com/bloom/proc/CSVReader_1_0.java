package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.Capabilities;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.source.classloading.ParserLoader;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.uuid.UUID;
import java.util.Map;
import org.apache.log4j.Logger;

@PropertyTemplate(name="CSVReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="wildcard", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="header", type=Boolean.class, required=false, defaultValue="False"), @com.bloom.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="64"), @com.bloom.anno.PropertyTemplateProperty(name="rowdelimiter", type=String.class, required=false, defaultValue="\n"), @com.bloom.anno.PropertyTemplateProperty(name="columndelimiter", type=String.class, required=false, defaultValue=","), @com.bloom.anno.PropertyTemplateProperty(name="eofdelay", type=Integer.class, required=false, defaultValue="100"), @com.bloom.anno.PropertyTemplateProperty(name="trimquote", type=Boolean.class, required=false, defaultValue="True"), @com.bloom.anno.PropertyTemplateProperty(name="quoteset", type=String.class, required=false, defaultValue="\""), @com.bloom.anno.PropertyTemplateProperty(name="positionbyeof", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8"), @com.bloom.anno.PropertyTemplateProperty(name="skipbom", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="expected_column_count", type=Integer.class, required=false, defaultValue="0")}, inputType=WAEvent.class)
@Capabilities(recovery=3)
public final class CSVReader_1_0
  extends BaseReader
{
  private static Logger logger = Logger.getLogger(CSVReader_1_0.class);
  private boolean isInitialized;
  private boolean mutliParamInit;
  
  public void init(Map<String, Object> propertyMap)
    throws Exception
  {
    if (!this.isInitialized)
    {
      this.isInitialized = true;
      
      String charSet = (String)propertyMap.get(Property.CHARSET);
      if ((charSet == null) || (charSet.isEmpty())) {
        propertyMap.put(Property.CHARSET, "UTF-8");
      }
      init(propertyMap, propertyMap, null, null, null, false, null);
    }
  }
  
  public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionID, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    if (!this.mutliParamInit)
    {
      this.mutliParamInit = true;
      if (logger.isDebugEnabled()) {
        logger.debug("CSVReader init with SourcePosition=" + startPosition);
      }
      prop1.put(ParserLoader.MODULE_NAME, "com.bloom.proc.DSVParser_1_0");
      prop2.put(Reader.READER_TYPE, Reader.FILE_READER);
      
      String charSet = (String)prop2.get(Property.CHARSET);
      if ((charSet == null) || (charSet.isEmpty())) {
        prop2.put(Property.CHARSET, "UTF-8");
      }
      super.init(prop1, prop2, uuid, distributionID, startPosition, sendPositions, flow);
    }
  }
  
  public void close()
    throws Exception
  {
    super.close();
    this.mutliParamInit = false;
    this.isInitialized = false;
  }
}
