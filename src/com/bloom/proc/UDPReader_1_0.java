package com.bloom.proc;


import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.WAEvent;
import com.bloom.source.lib.reader.Reader;

@PropertyTemplate(name="UDPReader", type=AdapterType.source, properties={@com.webaction.anno.PropertyTemplateProperty(name="IPAddress", type=String.class, required=true, defaultValue="localhost"), @com.webaction.anno.PropertyTemplateProperty(name="portno", type=Integer.class, required=true, defaultValue="10000"), @com.webaction.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="64"), @com.webaction.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.webaction.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8")}, inputType=WAEvent.class, requiresParser=true)
public class UDPReader_1_0
  extends BaseReader
{
  public UDPReader_1_0()
  {
    this.readerType = Reader.UDP_READER;
  }
}

