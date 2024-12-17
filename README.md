# activity_log_scala

### sbt build
```shell
sbt clean
sbt compile
sbt package
```

### spark-submit
#### 포멧
```
spark-submit --master local \
    --class "ActivityLogApp" \
    --deploy-mode (client|cluster) \
    --driver-memory <driver-memory> \
    --executor-memory <executor-memory> \
    <scala jar가 위치한 directory> "<input 파일 path - 1>" "<input 파일 path - 2>" ...
```

#### 예시
```
spark-submit --master local \
    --class "ActivityLogApp" \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    ./target/scala-2.12/activity-log-project_2.12-1.0.jar "./input/2019-Nov.csv" "./input/2019-Oct.csv"
```