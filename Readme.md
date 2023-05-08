
## Create a new Folder of tests

```bash
export type="<type of test>"
```

```bash
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
    -DarchetypeVersion=2.46.0 \
    -Dversion="0.1" \
    -DgroupId=com.mikenimer.swarm \
    -DartifactId=dataflow-$type-experiments \
    -Dpackage=com.mikenimer.swarm.$type \
    -DinteractiveMode=false
```
