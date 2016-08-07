package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.source.classloading.ParserLoader;
import com.bloom.source.lib.intf.Parser;
import com.bloom.uuid.UUID;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

@PropertyTemplate(name="MQTTReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="brokerUri", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="QoS", type=Integer.class, required=false, defaultValue="0"), @com.bloom.anno.PropertyTemplateProperty(name="clientId", type=String.class, required=false, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
public class MQTTReader_1_0
  extends BaseReader
  implements MqttCallback
{
  public static final String TOPIC_NAME_PROP_KEY = "Topic";
  public static final String MESSAGE_QOS_PROP_KEY = "QoS";
  public static final String BROKER_URI_PROP_KEY = "brokerUri";
  public static final String CLIENT_ID_PROP_KEY = "clientId";
  private static final Logger logger = Logger.getLogger(MQTTReader_1_0.class);
  private String brokerUri;
  private String topic;
  private int qos;
  private String clientId;
  private MqttClient mqttClient = null;
  private Parser parser = null;
  
  public void init(Map<String, Object> properties, Map<String, Object> parserProperties, UUID uuid, String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    super.init(properties, parserProperties, uuid, distributionID, restartPosition, sendPositions, flow);
    this.brokerUri = properties.get("brokerUri").toString();
    this.topic = properties.get("Topic").toString();
    this.qos = ((Integer)properties.get("QoS")).intValue();
    this.clientId = properties.get("clientId").toString();
    
    this.parser = ParserLoader.loadParser(parserProperties, uuid);
    
    this.mqttClient = new MqttClient(this.brokerUri, this.clientId);
    this.mqttClient.setCallback(this);
    this.mqttClient.connect();
    this.mqttClient.subscribe(this.topic, this.qos);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {}
  
  public void close()
    throws Exception
  {
    super.close();
    if (this.mqttClient != null) {
      this.mqttClient.disconnect();
    }
  }
  
  public void connectionLost(Throwable t)
  {
    try
    {
      this.mqttClient.connect();
      this.mqttClient.subscribe(this.topic, this.qos);
    }
    catch (Exception e)
    {
      logger.warn("Unable to reconnect/subscribe to topic : " + this.topic, e);
    }
  }
  
  public void deliveryComplete(IMqttDeliveryToken token)
  {
    logger.debug("Successful delivery  " + token.toString());
  }
  
  public void messageArrived(String topic, MqttMessage message)
    throws Exception
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Message arrived " + message.toString());
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(message.getPayload());
    Iterator<Event> iterator = this.parser.parse(bis);
    while (iterator.hasNext())
    {
      Event evt = (Event)iterator.next();
      send(evt, 0);
    }
  }
  
  public static void main(String[] args)
  {
    Map<String, Object> props = new HashMap();
    
    props.put("handler", "JSONParser");
    props.put("Topic", "/tmp/subtopic");
    props.put("QoS", Integer.valueOf(0));
    props.put("brokerUri", "tcp://m2m.eclipse.org:1883");
    props.put("clientId", "STRIIM");
    
    MQTTReader_1_0 reader = new MQTTReader_1_0();
    try
    {
      reader.init(props, props, null, null, null, false, null);
      
      reader.addEventSink(new AbstractEventSink()
      {
        public void receive(int channel, Event event)
          throws Exception
        {
          System.out.println("******Received Event***********");
          System.out.print(event);
          System.out.println("*******************************");
        }
      });
    }
    catch (Exception e)
    {
      e.printStackTrace();
      logger.error(e);
    }
  }
}
