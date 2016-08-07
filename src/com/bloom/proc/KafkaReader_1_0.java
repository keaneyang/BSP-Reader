package com.bloom.proc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.exc.ConnectionException;
import com.bloom.common.exc.MetadataUnavailableException;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.KafkaSourcePosition;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.source.kafka.KafkaPartitionHandler;
import com.bloom.source.kafka.KafkaProperty;
import com.bloom.source.kafka.utils.KafkaUtils;
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.type.positiontype;
import com.bloom.uuid.UUID;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.KafkaException;
import org.apache.log4j.Logger;

@PropertyTemplate(name="KafkaReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="brokerAddress", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="10240"), @com.bloom.anno.PropertyTemplateProperty(name="Topic", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="PartitionIDList", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8"), @com.bloom.anno.PropertyTemplateProperty(name="startOffset", type=Integer.class, required=false, defaultValue="-1"), @com.bloom.anno.PropertyTemplateProperty(name="KafkaConfig", type=String.class, required=false, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
public class KafkaReader_1_0
  extends BaseReader
{
  public static final String DISTRIBUTION_ID = "distributionId";
  private static final Logger logger = Logger.getLogger(KafkaReader_1_0.class);
  private String topic;
  private List<Integer> partitionIdList;
  private UUID sourceRef;
  private Map<String, Object> localCopyOfProperty;
  private boolean stopFetching = false;
  private int blocksize;
  private KafkaProperty prop;
  private KafkaSourcePosition kafkaSourcePosition;
  private boolean sendPositions = false;
  private TopicMetadata topicMetaData;
  private positiontype posType;
  public Map<Integer, KafkaPartitionHandler> partitionMap;
  private int partitionCount = 0;
  private String distributionId = null;
  private long retryBackoffms = 1000L;
  private HashSet<String> replicaList;
  private ExecutorService threadPool;
  
  public KafkaReader_1_0()
  {
    this.kafkaSourcePosition = new KafkaSourcePosition();
  }
  
  public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    super.init(prop1, prop2, uuid, distributionId);
    this.sourceRef = uuid;
    this.localCopyOfProperty = new HashMap();
    this.localCopyOfProperty.putAll(prop1);
    this.localCopyOfProperty.putAll(prop2);
    this.localCopyOfProperty.put(BaseReader.SOURCE_PROCESS, this);
    this.localCopyOfProperty.put(Property.SOURCE_UUID, this.sourceRef);
    this.localCopyOfProperty.put("distributionId", distributionId);
    this.localCopyOfProperty.put("so_timeout", Integer.valueOf(30010));
    
    this.prop = new KafkaProperty(this.localCopyOfProperty);
    
    this.topic = this.prop.topic;
    String partitionList = this.prop.partitionList;
    this.blocksize = (this.prop.blocksize * 1024);
    this.stopFetching = false;
    
    this.distributionId = distributionId;
    this.sendPositions = sendPositions;
    this.partitionCount = 0;
    
    String[] kafkaConfig = this.prop.getKafkaBrokerConfigList();
    if (kafkaConfig != null) {
      for (int i = 0; i < kafkaConfig.length; i++)
      {
        String[] property = kafkaConfig[i].split("=");
        if ((property == null) || (property.length < 2))
        {
          logger.warn("Kafka Property \"" + property[0] + "\" is invalid.");
          logger.warn("Invalid \"KafkaConfig\" property structure " + property[0] + ". Expected structure <name>=<value>;<name>=<value>");
        }
        else if (property[0].equals("retry.backoff.ms"))
        {
          this.retryBackoffms = Long.parseLong(property[1]);
        }
      }
    }
    boolean topicMetdaDataFound = false;
    if (this.prop.kafkaBrokerAddress != null)
    {
      this.replicaList = new HashSet();
      for (int i = 0; i < this.prop.kafkaBrokerAddress.length; i++) {
        this.replicaList.add(this.prop.kafkaBrokerAddress[i]);
      }
      for (int i = 0; (i < this.prop.kafkaBrokerAddress.length) && (!topicMetdaDataFound); i++) {
        try
        {
          String[] ipAndPort = this.prop.kafkaBrokerAddress[i].split(":");
          String brokerIpAddres = ipAndPort[0];
          int brokerPortNo = Integer.parseInt(ipAndPort[1]);
          
          this.topicMetaData = KafkaUtils.lookupTopicMetadata(brokerIpAddres, brokerPortNo, this.topic, this.retryBackoffms);
          if (this.topicMetaData != null) {
            topicMetdaDataFound = true;
          }
        }
        catch (Exception e)
        {
          logger.error(e);
          if (i == this.prop.kafkaBrokerAddress.length - 1) {
            throw new MetadataUnavailableException("Failure in getting metadata for Kafka Topic. Closing the Kafka Reader " + e);
          }
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.info(this.topicMetaData);
    }
    if ((partitionList != null) && (!partitionList.trim().isEmpty()))
    {
      this.partitionIdList = new ArrayList();
      if (partitionList.contains(";"))
      {
        String[] partitionIds = partitionList.split(";");
        for (int i = 0; i < partitionIds.length; i++)
        {
          this.partitionIdList.add(Integer.valueOf(Integer.parseInt(partitionIds[i])));
          this.partitionCount += 1;
        }
      }
      else
      {
        this.partitionIdList.add(Integer.valueOf(Integer.parseInt(partitionList)));
        this.partitionCount += 1;
      }
    }
    else
    {
      if (logger.isInfoEnabled()) {
        logger.info("No partitions were specified for the topic - " + this.topic + ". Trying to fetch partition list from Topic Metadata");
      }
      this.partitionCount = this.topicMetaData.partitionsMetadata().size();
      this.partitionIdList = new ArrayList();
      for (int i = 0; i < this.partitionCount; i++)
      {
        int partitionId = ((PartitionMetadata)this.topicMetaData.partitionsMetadata().get(i)).partitionId();
        this.partitionIdList.add(Integer.valueOf(partitionId));
      }
      if (this.partitionIdList.isEmpty()) {
        throw new IllegalArgumentException("Unable to fetch the parition IDs for the topic - " + this.topic);
      }
    }
    if (this.sendPositions)
    {
      if (this.partitionCount > 1)
      {
        this.sendPositions = false;
        this.posType = (this.prop.posType = positiontype.WA_POSITION_EOF);
        logger.warn("KafkaReader supports recovery for a topic containing one partition only. Since the topic : " + this.topic + " contains " + this.partitionCount + " partitions, KafkaReader is currently positioned to read from End of each partitions.");
      }
      else if ((startPosition != null) && ((startPosition instanceof KafkaSourcePosition)))
      {
        this.kafkaSourcePosition = new KafkaSourcePosition((KafkaSourcePosition)startPosition);
        if (logger.isDebugEnabled()) {
          logger.info("Restart position " + this.kafkaSourcePosition.toString());
        }
        this.posType = positiontype.WA_POSITION_OFFSET;
      }
      else
      {
        this.posType = this.prop.posType;
      }
    }
    else {
      this.posType = this.prop.posType;
    }
    ThreadFactory kafkaParserThreadFactory = new ThreadFactoryBuilder().setNameFormat("KafkaParserThread-%d").build();
    this.threadPool = Executors.newCachedThreadPool(kafkaParserThreadFactory);
    
    initializeConsumer(this.topicMetaData);
    if (logger.isInfoEnabled()) {
      logger.info("Data will be consumed from topic - " + this.topic + "(partitions - " + this.partitionIdList.toString() + ")");
    }
  }
  
  public String getTopic()
  {
    return this.topic;
  }
  
  public KafkaSourcePosition getSourcePosition()
  {
    return this.kafkaSourcePosition;
  }
  
  public void initializeConsumer(TopicMetadata topicMetaData)
    throws Exception
  {
    int noOfInvalidParitionId = 0;
    
    this.partitionMap = new HashMap();
    for (Integer partitionId : this.partitionIdList)
    {
      boolean recovery = false;
      long kafkaReadOffset = 0L;
      long waReadOffset = 0L;
      PartitionMetadata partitionMetadata = KafkaUtils.getPartitionMetadata(topicMetaData, partitionId.intValue());
      if (partitionMetadata == null)
      {
        logger.warn("Partition with ID " + partitionId + " does not exist in the topic " + this.topic);
        noOfInvalidParitionId++;
      }
      else
      {
        if (this.posType == positiontype.WA_POSITION_OFFSET)
        {
          String key = this.topic + ":" + partitionId;
          if (this.kafkaSourcePosition.getCurrentPartitionID().equals(key))
          {
            kafkaReadOffset = this.kafkaSourcePosition.getKafkaReadOffset();
            waReadOffset = this.kafkaSourcePosition.getRecordEndOffset();
            recovery = true;
            if (logger.isInfoEnabled()) {
              logger.info("Restart position available. Kafka Read offset is " + kafkaReadOffset + " and WA Record end offset is " + waReadOffset + " for " + this.topic + "-" + partitionId);
            }
          }
          else
          {
            kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId.intValue(), OffsetRequest.EarliestTime(), partitionMetadata.leader().host(), partitionMetadata.leader().port());
            waReadOffset = 0L;
            if (logger.isInfoEnabled()) {
              logger.info("Restart position was not found. Hence reading from beginning. Read offset is " + kafkaReadOffset + " for " + this.topic + "-" + partitionId);
            }
          }
        }
        else if (this.posType == positiontype.WA_POSITION_EOF)
        {
          kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId.intValue(), OffsetRequest.LatestTime(), partitionMetadata.leader().host(), partitionMetadata.leader().port());
          waReadOffset = 0L;
          if (logger.isInfoEnabled()) {
            logger.info("Restart by EOF (End of File). Read offset is " + kafkaReadOffset + " for " + this.topic + "-" + partitionId);
          }
        }
        else if (this.posType == positiontype.WA_POSITION_SOF)
        {
          kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, partitionId.intValue(), OffsetRequest.EarliestTime(), partitionMetadata.leader().host(), partitionMetadata.leader().port());
          waReadOffset = 0L;
          if (logger.isInfoEnabled()) {
            logger.info("Restart by SOF (Start of File). Read offset is " + kafkaReadOffset + " for " + this.topic + "-" + partitionId);
          }
        }
        KafkaPartitionHandler metadata = new KafkaPartitionHandler(this.topic, partitionMetadata, kafkaReadOffset, waReadOffset, this.prop);
        
        metadata.hasPositioned(recovery);
        
        metadata.sendPosition(this.sendPositions);
        
        this.replicaList.addAll(metadata.replicaBrokers);
        this.partitionMap.put(partitionId, metadata);
        
        this.threadPool.submit(metadata);
      }
    }
    if (this.partitionIdList.size() == noOfInvalidParitionId) {
      throw new MetadataUnavailableException("Failure in Kafka Connection due to invalid Topic name or partition id. PartitionIDs " + this.partitionIdList.toString() + "does not exist in the Topic " + this.topic + ". Please check your topic name and number of partitions in it.");
    }
  }
  
  public void startConsumingData()
    throws Exception
  {
    ByteBuffer payload = ByteBuffer.allocate(this.blocksize + Constant.INTEGER_SIZE + Constant.LONG_SIZE + Constant.BYTE_SIZE);
    int emptyPartitionCount = 0;
    try
    {
      while (!this.stopFetching)
      {
        if (this.partitionMap.isEmpty())
        {
          this.stopFetching = true;
          logger.warn("No Kafka Brokers available to fetch data ");
          throw new ConnectionException("Failure in connecting to Kafka broker. No Kafka Brokers available to fetch data.");
        }
        Iterator it = this.partitionMap.entrySet().iterator();
        while (it.hasNext())
        {
          Map.Entry item = (Map.Entry)it.next();
          KafkaPartitionHandler metadata = (KafkaPartitionHandler)item.getValue();
          if (this.stopFetching)
          {
            if (metadata.consumer != null)
            {
              metadata.consumer.close();
              metadata.consumer = null;
            }
            return;
          }
          if (metadata.consumer == null) {
            metadata.consumer = new SimpleConsumer(metadata.leaderIp, metadata.leaderPort, 30010, this.blocksize, metadata.clientName);
          }
          FetchRequest req = new FetchRequestBuilder().clientId(metadata.clientName).addFetch(this.topic, metadata.partitionId, metadata.kafkaReadOffset, this.blocksize).maxWait(10).build();
          
          FetchResponse fetchResponse = null;
          try
          {
            fetchResponse = metadata.consumer.fetch(req);
          }
          catch (Exception exception)
          {
            if (!this.stopFetching)
            {
              if (((exception instanceof EOFException)) || ((exception instanceof ClosedChannelException)))
              {
                if (metadata.getLeaderLookUpCount() > 5)
                {
                  logger.warn("Failure in fetching data from [" + this.topic + "," + metadata.partitionId + "] since leader is not available.");
                  it.remove();
                  metadata.cleanUp();
                  continue;
                }
                metadata.increaseLeaderLookUpCount(1);
                
                logger.warn("Leader (" + metadata.leaderIp + ":" + metadata.leaderPort + ") is dead. Try to find the new Leader for [" + this.topic + "," + metadata.partitionId + "].");
                try
                {
                  PartitionMetadata partitionMetadata = KafkaUtils.findNewLeader(this.topic, metadata.partitionId, metadata.replicaBrokers, metadata.leaderIp, metadata.leaderPort, this.replicaList, this.retryBackoffms);
                  long newReadOffset = metadata.kafkaReadOffset;
                  if (partitionMetadata != null)
                  {
                    metadata.updatePartitionMetadata(partitionMetadata, newReadOffset);
                    this.replicaList.addAll(metadata.replicaBrokers);
                    it.remove();
                    this.partitionMap.put(Integer.valueOf(metadata.partitionId), metadata);
                    if (logger.isInfoEnabled()) {
                      logger.info("Consumer " + metadata.clientName + " will start consuming data from (" + metadata.leaderIp + ":" + metadata.leaderPort + ")" + "for (" + this.topic + "-" + metadata.partitionId + ") from Offset " + metadata.kafkaReadOffset + ".");
                    }
                    break;
                  }
                  logger.warn("Could not find a new Leader for [" + this.topic + "," + metadata.partitionId + "].");
                }
                catch (Exception e)
                {
                  logger.warn("Unable to find new Leader for [" + this.topic + "," + metadata.partitionId + "].");
                  logger.warn(e);
                  metadata.cleanUp();
                  it.remove();
                }
              }
            }
            else
            {
              if (logger.isInfoEnabled()) {
                logger.info("Shutting down Bloom Kafka consumer");
              }
              break;
            }
          }
          continue;
          if (fetchResponse.hasError())
          {
            metadata.increaseErrorCount(1);
            
            short code = fetchResponse.errorCode(this.topic, metadata.partitionId);
            if (code == ErrorMapping.InvalidTopicCode())
            {
              Throwable cause = ErrorMapping.exceptionFor(code);
              String message = cause.getMessage();
              logger.error("Topic name is Invalid. " + message);
              throw new MetadataUnavailableException(this.topic + "Topic is Invalid.", cause);
            }
            if (code == ErrorMapping.OffsetOutOfRangeCode())
            {
              Throwable cause = ErrorMapping.exceptionFor(code);
              KafkaException k = new KafkaException("Invalid read offset" + metadata.kafkaReadOffset, cause);
              logger.error(k.getCause() + " " + k.getMessage());
              metadata.kafkaReadOffset = KafkaUtils.getPartitionOffset(this.topic, metadata.partitionId, OffsetRequest.LatestTime(), metadata.leaderIp, metadata.leaderPort);
              if (logger.isInfoEnabled()) {
                logger.info("Updated the read Offset of [" + this.topic + "," + metadata.partitionId + "] to " + metadata.kafkaReadOffset);
              }
            }
            else if (code == ErrorMapping.NotLeaderForPartitionCode())
            {
              if (logger.isInfoEnabled()) {
                logger.info("Leader of [" + this.topic + "," + metadata.partitionId + "] has changed. Trying to find the new Leader for [" + this.topic + "," + metadata.partitionId + "].");
              }
              if (metadata.getLeaderLookUpCount() > 5)
              {
                logger.warn("Unable to fetch data from [" + this.topic + "," + metadata.partitionId + "] since leader is not available.");
                it.remove();
                metadata.cleanUp();
              }
              else
              {
                metadata.increaseLeaderLookUpCount(1);
                try
                {
                  PartitionMetadata partitionMetadata = KafkaUtils.findNewLeader(this.topic, metadata.partitionId, metadata.replicaBrokers, metadata.leaderIp, metadata.leaderPort, this.replicaList, this.retryBackoffms);
                  long newReadOffset = metadata.kafkaReadOffset;
                  if (partitionMetadata != null)
                  {
                    metadata.updatePartitionMetadata(partitionMetadata, newReadOffset);
                    this.replicaList.addAll(metadata.replicaBrokers);
                    it.remove();
                    this.partitionMap.put(Integer.valueOf(metadata.partitionId), metadata);
                    if (logger.isInfoEnabled()) {
                      logger.info("Consumer " + metadata.clientName + " will start consuming data from (" + metadata.leaderIp + ":" + metadata.leaderPort + ")" + "for (" + this.topic + "-" + metadata.partitionId + ") from Offset " + metadata.kafkaReadOffset + ".");
                    }
                    break;
                  }
                  logger.warn("Could not find a new Leader for [" + this.topic + "," + metadata.partitionId + "].");
                }
                catch (Exception e)
                {
                  logger.warn("Unable to find new Leader for [" + this.topic + "," + metadata.partitionId + "].");
                  logger.warn(e);
                  metadata.cleanUp();
                  it.remove();
                }
              }
            }
            else if (metadata.noOfErrors() > 10)
            {
              Throwable cause = ErrorMapping.exceptionFor(code);
              KafkaException k = new KafkaException("Fetching response for [" + this.topic + "," + metadata.partitionId + "] has a error ", cause);
              logger.error(k.getCause() + " " + k.getMessage());
              logger.warn("Stopped fetching data from [" + this.topic + "," + metadata.partitionId + "]");
              metadata.cleanUp();
              it.remove();
            }
          }
          else
          {
            if (fetchResponse.messageSet(this.topic, metadata.partitionId).sizeInBytes() > 0) {
              for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.topic, metadata.partitionId))
              {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < metadata.kafkaReadOffset)
                {
                  logger.warn("Found an old offset: " + currentOffset + " Expecting: " + metadata.kafkaReadOffset);
                }
                else
                {
                  metadata.kafkaReadOffset = messageAndOffset.nextOffset();
                  if (messageAndOffset.message().payloadSize() > 0)
                  {
                    payload.clear();
                    if (metadata.isRecovery())
                    {
                      long endOffset = metadata.waReadOffset;
                      ByteBuffer tmpBuffer = ByteBuffer.allocate(messageAndOffset.message().payloadSize());
                      tmpBuffer.put(messageAndOffset.message().payload());
                      tmpBuffer.flip();
                      CharBuffer charBuf = CharBuffer.allocate(messageAndOffset.message().payloadSize());
                      
                      CharsetDecoder decoder = Charset.forName(this.prop.charset).newDecoder();
                      CoderResult result = decoder.decode(tmpBuffer, charBuf, true);
                      if (!result.isError())
                      {
                        charBuf.flip();
                        if (charBuf.length() > endOffset)
                        {
                          int bytesToBeProcessed = charBuf.length() - (int)endOffset;
                          if (bytesToBeProcessed > 0)
                          {
                            CharBuffer tmpCharBuf = CharBuffer.allocate(bytesToBeProcessed);
                            System.arraycopy(charBuf.array(), (int)endOffset, tmpCharBuf.array(), 0, bytesToBeProcessed);
                            tmpBuffer.clear();
                            tmpBuffer = Charset.forName(this.prop.charset).encode(tmpCharBuf);
                            int messageSize = tmpBuffer.limit();
                            payload.putLong(currentOffset);
                            payload.put((byte)1);
                            payload.putInt(messageSize);
                            payload.put(tmpBuffer);
                            payload.flip();
                            writeData(messageSize, payload, metadata);
                          }
                        }
                        else if (logger.isInfoEnabled())
                        {
                          logger.info("Skipping the kafka message with offset " + metadata.kafkaReadOffset + " since the message wasa already processed.");
                        }
                      }
                      else
                      {
                        logger.warn("Unable to decode the kafka message with offset " + metadata.kafkaReadOffset + ". Skipping to the next message");
                      }
                      metadata.hasPositioned(false);
                      charBuf = null;
                      tmpBuffer = null;
                    }
                    else
                    {
                      int messageSize = messageAndOffset.message().payloadSize();
                      payload.putLong(currentOffset);
                      payload.put((byte)0);
                      payload.putInt(messageSize);
                      payload.put(messageAndOffset.message().payload());
                      payload.flip();
                      writeData(messageSize, payload, metadata);
                    }
                  }
                }
              }
            } else {
              emptyPartitionCount++;
            }
            metadata = null;
            if (emptyPartitionCount % this.partitionCount == 0)
            {
              Thread.sleep(10L);
              if (emptyPartitionCount == 5000) {
                logger.warn("No messages were consumed from the topic '" + this.topic + "' (partitions - " + this.partitionIdList + ") for a while. " + "Please check if the value of \"blockSize\" property specified as " + this.blocksize + " bytes is greater than \"max.message.bytes\" (in topic configuration) or" + " \"message.max.bytes\" (in broker configuration file)." + " Please ignore this warning if no message was produced to the Topic.");
              }
            }
          }
        }
      }
    }
    catch (Exception e)
    {
      if (((e instanceof InterruptedException)) || ((e instanceof InterruptedIOException)))
      {
        if (logger.isDebugEnabled()) {
          logger.info(e);
        }
      }
      else {
        throw new ConnectionException(e);
      }
    }
    finally
    {
      cleanUp();
    }
  }
  
  public void writeData(int messageSize, ByteBuffer payload, KafkaPartitionHandler metadata)
    throws IOException
  {
    int totalMessageSize = messageSize + Constant.BYTE_SIZE + Constant.INTEGER_SIZE + Constant.LONG_SIZE;
    byte[] bytes = new byte[totalMessageSize];
    
    payload.get(bytes, 0, totalMessageSize);
    
    logger.info("Message payload (time=\" + System.currentTimeMillis() + \") " + new String(bytes, 12, messageSize));
    if (metadata.pipedOut != null) {
      metadata.pipedOut.write(bytes);
    }
  }
  
  public void receiveImpl(int channel, Event out)
    throws Exception
  {
    try
    {
      synchronized (this)
      {
        if (!this.stopFetching) {
          startConsumingData();
        }
      }
    }
    catch (Exception e)
    {
      logger.error(e);
      throw new ConnectionException("Failure in Kafka Connection. Closing the KafkaReader. ", e);
    }
  }
  
  public void cleanUp()
  {
    this.stopFetching = true;
    this.threadPool.shutdownNow();
    Iterator it = this.partitionMap.entrySet().iterator();
    while (it.hasNext()) {
      try
      {
        Map.Entry item = (Map.Entry)it.next();
        KafkaPartitionHandler pm = (KafkaPartitionHandler)item.getValue();
        pm.stopFetching(true);
        pm.cleanUp();
        it.remove();
      }
      catch (Exception e)
      {
        logger.warn(e.getMessage());
      }
    }
  }
  
  public void close()
    throws Exception
  {
    this.stopFetching = true;
  }
  
  public static void main(String[] args)
  {
    HashMap<String, Object> mr = new HashMap();
    
    mr.put("brokerAddress", "localhost:9092");
    mr.put("startOffset", Integer.valueOf(0));
    
    mr.put("Topic", "admin_PosData");
    
    mr.put("blocksize", "256");
    mr.put("KafkaConfig", "retry.backoff.ms=1000");
    
    HashMap<String, Object> mp = new HashMap();
    
    mp.put("handler", "KryoParser");
    
    mr.put("positionByEOF", Boolean.valueOf(false));
    
    mp.put("schemaFileName", "/Users/vino/dev/Product/avrotest_1.avsc");
    KafkaReader_1_0 kf = new KafkaReader_1_0();
    System.out.println("Gonna initialize");
    UUID uuid = new UUID(System.currentTimeMillis());
    mp.put(Property.SOURCE_UUID, uuid);
    try
    {
      kf.init(mr, mp, uuid, null, null, false, null);
      kf.receiveImpl(0, null);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
