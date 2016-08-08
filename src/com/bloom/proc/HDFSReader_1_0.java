package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.WAEvent;
import com.bloom.source.lib.reader.Reader;

@PropertyTemplate(name="HDFSReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="hadoopurl", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="wildcard", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="positionbyeof", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="eofdelay", type=Integer.class, required=false, defaultValue="100"), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8"), @com.bloom.anno.PropertyTemplateProperty(name="skipbom", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="rolloverstyle", type=String.class, required=false, defaultValue="Default"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="authenticationpolicy", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="hadoopconfigurationpath", type=String.class, required=false, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
public class HDFSReader_1_0
  extends BaseReader
{
  public HDFSReader_1_0()
  {
    this.readerType = Reader.HDFS_READER;
  }
}
