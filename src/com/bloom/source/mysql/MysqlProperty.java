package com.bloom.source.mysql;

import com.bloom.source.lib.prop.Property;
import java.util.Map;
import org.apache.log4j.Logger;

public class MysqlProperty
  extends Property
{
  private Map<String, Object> propMap;
  private String binlogFileName = null;
  private String previousBinlogFileName = null;
  private String schema = null;
  private Long binLogPosition;
  private Logger logger = Logger.getLogger(MysqlProperty.class);
  
  public MysqlProperty(Map<String, Object> map)
  {
    super(map);
    this.propMap = map;
    this.binlogFileName = ((String)this.propMap.get("BinlogFileName"));
    this.binLogPosition = Long.valueOf(4L);
    if (this.propMap.containsKey("BinlogPosition")) {
      this.binLogPosition = Long.valueOf(Long.parseLong(this.propMap.get("BinlogPosition").toString()));
    }
    if (this.binLogPosition.longValue() < 4L)
    {
      this.logger.warn("Invalid Binary Log position " + this.binLogPosition + ". Updating Binary Log position to 4");
      this.binLogPosition = Long.valueOf(4L);
    }
  }
  
  public String getbinlogFileName()
  {
    return this.binlogFileName;
  }
  
  public Long getbinLogPosition()
  {
    return this.binLogPosition;
  }
  
  public String getSchema()
  {
    return this.schema;
  }
  
  public void setBinlogFileName(String fileName)
  {
    this.binlogFileName = fileName;
  }
  
  public void setBinLogPosition(Long position)
  {
    this.binLogPosition = position;
  }
  
  public String getPreviousBinlogFileName()
  {
    return this.previousBinlogFileName;
  }
  
  public void setPreviousBinlogFileName(String previousBinlogFileName)
  {
    this.previousBinlogFileName = previousBinlogFileName;
  }
}
