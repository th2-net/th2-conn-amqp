# th2-conn-amqp

The project contains the implementation of an AMQP connection **th2-conn-amqp**.

# Connection properties example

* initialContextFactory = org.apache.qpid.jms.jndi.JmsInitialContextFactory
* factorylookup = amqp://\<host\>:\<port\>?jms.username=\<username\>&jms.password=\<password\>

> for TLS the factorylookup will look like below
> factorylookup = amqps://&lt;url&gt;:&lt;port&gt;?sslEnabled=true&trustStorePath=&lt;trustStore&gt;.
> jks&trustStorePassword=&lt;trustStorePassword&gt;&keyStorePath=&lt;keyStorePath&gt;.
> jks&keyStorePassword=&lt;keyStorePassword&gt;&saslMechanisms=EXTERNAL

* sendQueue = \<sendQueue\>
* receiveQueue = \<receiveQueue\>

# Pins

The th2-conn box has 3 types of pins:
+ raw messages that goes from the th2-conn to the system;
+ raw messages that goes from the system to the th2-conn;
+ messages to send that goes from user the th2-conn.

Configuration example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: conn-amqp
spec:
  image-name: your.image.repo:42/th2-conn-amqp
  image-version: 0.0.1
  type: th2-conn
  custom-config:
    drainIntervalMills: 1000
    rootEventName: ConnAmqp
    sessions:
    - 
      # sessionAlias:
      # initialContextFactory:
      # factorylookup:
      # sendQueue:
      # receiveQueue:
    - 
      # sessionAlias:
      # initialContextFactory:
      # factorylookup:
      # sendQueue:
      # receiveQueue:

# Deprecated and will be deleted in the next major version.
# Instead of sessions there is a way to use parameters with sessionAlias for single connection
# But you can use only one of these ways
  sessionAlias: session-alias
  parameters:
        # initialContextFactory:
        # factorylookup:
        # sendQueue:
        # receiveQueue:

  pins:
    - name: in_raw
      connection-type: mq
      attributes: ["first", "raw", "publish", "store"]
    - name: out_raw
      connection-type: mq
      attributes: ["second", "raw", "publish", "store"]
    - name: to_send
      connection-type: mq
      attributes: ["send", "parsed", "subscribe"]
```

# Release notes
## 1.2.0
+ Add possibility of multiple connections
    + Add list of sessions with connection parameters
    + Backward compatibility remains
    
