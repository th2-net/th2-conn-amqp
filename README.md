# Template project for th2-conn

The project contains the base structure that is required for creation a **th2-conn**.

The minimal set of the required and useful dependencies is added to the `build.gradle` file.

# What do you need to change?

If you are using this template for creating your own conn box please do the following steps before starting the actual development:
+ Change the **rootProject.name** in `settings.gradle` file. The name **should not** contain the **th2** prefix;
+ Change the **APP_NAME** in the `.gitlab-ci.yml` file. It should be the same as project name but with **th2** prefix;
+ Change the value for **DOCKER_PUBLISH_ENABLED** in the `.gitlab-ci.yml` file to enable docker image publication;
+ Change the package name from `template` to a different name. It probably should be the same as the box name;
+ Correct the following block in the `build.gradle` file according to the previous step
    ```groovy
    application {
        mainClass.set('com.exactpro.th2.conn.ampq.BoxMain')
    }
    ```
After that you will need to implement the actual logic for the connect component.

The actual implementation should be written in the `com.exactpro.th2.conn.ampq.impl.ConnServiceImpl` class.

Your custom parameters should be specified in the `com.exactpro.th2.conn.ampq.impl.ConnParameters` class.

# Pins

The th2-conn box has 3 types of pins:
+ raw messages that goes from the th2-conn to the system;
+ raw messages that goes from the system to the th2-conn;
+ messages to send that goes from user the th2-conn.

Configuration example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
spec:
  image-name: your.image.repo:42/th2-conn-template
  image-version: 0.0.1
  type: th2-conn
  custom-config:
      sessionAlias: session-alias
      drainIntervalMills: 1000
      rootEventName: ConnTemplate
      parameters:
          # your custom parameters here if you need them
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

# Useful links

+ th2-common - https://github.com/th2-net/th2-common-j