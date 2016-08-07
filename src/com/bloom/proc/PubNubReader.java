package com.bloom.proc;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.uuid.UUID;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;

@PropertyTemplate(name="PubNubReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="pub_key", type=String.class, required=false, defaultValue="demo"), @com.bloom.anno.PropertyTemplateProperty(name="sub_key", type=String.class, required=true, defaultValue="demo"), @com.bloom.anno.PropertyTemplateProperty(name="channel", type=String.class, required=true, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
public class PubNubReader
  extends BaseReader
{
  Pubnub pubnub = null;
  String channel = null;
  PipedInputStream reader;
  PipedOutputStream writer;
  
  class PubNubHandler
    extends Callback
  {
    PubNubHandler() {}
    
    public void successCallback(String channel, Object message)
    {
      try
      {
        PubNubReader.this.writer.write(message.toString().getBytes());
      }
      catch (IOException e) {}
    }
    
    public void successCallback(String channel, Object message, String timetoken)
    {
      try
      {
        PubNubReader.this.writer.write(message.toString().getBytes());
      }
      catch (IOException e) {}
    }
    
    public void errorCallback(String channel, PubnubError error) {}
    
    public void connectCallback(String channel, Object message) {}
    
    public void reconnectCallback(String channel, Object message) {}
    
    public void disconnectCallback(String channel, Object message) {}
  }
  
  public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    this.reader = new PipedInputStream();
    this.writer = new PipedOutputStream(this.reader);
    
    super.init(prop1, prop2, uuid, distributionId, startPosition, sendPositions, flow);
    
    String pub_key = prop1.get("pub_key").toString();
    String sub_key = prop1.get("sub_key").toString();
    this.channel = prop1.get("channel").toString();
    this.pubnub = new Pubnub(pub_key, sub_key);
    
    this.pubnub.subscribe(this.channel, new PubNubHandler());
  }
  
  public void close()
    throws Exception
  {
    this.pubnub.unsubscribe(this.channel);
    this.pubnub = null;
    super.close();
    this.writer.close();
    this.reader.close();
  }
  
  public InputStream createInputStream(Map<String, Object> prop)
    throws Exception
  {
    return this.reader;
  }
}

