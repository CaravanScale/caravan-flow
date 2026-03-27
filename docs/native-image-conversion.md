# Native Image Conversion: Static Analysis + Automated Test Generation

> **Status**: DESIGN COMPLETE — ready for implementation
> **Scope**: Static analysis tool that scans source code to generate GraalVM native-image configs AND automated test harnesses for zinc-flow and NiFi processors

## The Problem

GraalVM native-image does ahead-of-time (AOT) compilation. It needs to know at build time every class that will ever be used at runtime. Java libraries use reflection extensively:

```java
Class.forName("com.fasterxml.jackson.databind.ObjectMapper")  // resolved at runtime
clazz.getMethod("serialize")                                   // unknown at build time
```

GraalVM can't see these at compile time, so it excludes them. The native binary crashes at runtime.

### Why static analysis, not the tracing agent

The standard GraalVM approach is a **tracing agent** — run your app on a normal JVM, exercise it, and the agent records every reflective call it observes. This is a black-box approach designed for when you **don't have source code**.

We have all the source code: zinc-flow, NiFi processors, and every open-source dependency. Static analysis is superior:

| | Tracing agent | Static analysis |
|---|---|---|
| **Completeness** | Only captures exercised paths — miss a path, miss the reflection | Finds ALL reflection call sites in ALL code paths |
| **Determinism** | Depends on test quality and runtime conditions | Same source always produces same result |
| **Speed** | Must stand up services, send requests, wait | Scans source files directly |
| **Reproducibility** | Flaky — timing, network, state affect results | Fully reproducible |
| **Transitive depth** | Captures whatever happens to execute | Traces call chains to arbitrary depth |
| **Test generation** | Produces configs only | Produces configs AND test harnesses |

The tracing agent is a fallback for truly black-box native code (JNI libraries, closed-source JARs). For everything else, static analysis gives us more.

## Design: `reflect-scan` — Static Reflection Analyzer

### What it does

`reflect-scan` walks source code (Java and Zinc-transpiled Java) and:

1. **Finds all reflection call sites** — `Class.forName`, `getMethod`, `newInstance`, `ServiceLoader.load`, `Proxy.newProxyInstance`, etc.
2. **Resolves arguments statically** — most reflection calls use string literals (`Class.forName("com.foo.Bar")`), enum constants, or values traceable through assignments
3. **Follows call chains transitively** — if method A calls method B which calls `Class.forName`, the tool traces A -> B -> reflection site, across class and JAR boundaries
4. **Builds a reflection graph** — a complete map of: call site -> reflected class/method -> what triggered it -> full transitive chain
5. **Generates GraalVM configs** — `reflect-config.json`, `resource-config.json`, `jni-config.json`, `proxy-config.json`, `serialization-config.json`
6. **Generates test harnesses** — for every reflection path found, generate a test that exercises that exact code path

### Architecture

```
Source code (.java / .zn)
        |
        v
   [reflect-scan]
        |
        +---> Reflection Graph (complete call-chain map)
        |         |
        |         +---> reflect-config.json  (GraalVM)
        |         +---> resource-config.json (GraalVM)
        |         +---> jni-config.json      (GraalVM)
        |         +---> proxy-config.json    (GraalVM)
        |         +---> serialization-config.json (GraalVM)
        |         |
        |         +---> test harness (exercises every reflection path)
        |         +---> compatibility report (GREEN/YELLOW/RED per module)
        |
        v
   native-image --no-fallback -H:ConfigurationFileDirectories=./graal-config
```

### Reflection patterns to detect

**Direct reflection (depth 0):**

| Pattern | What to extract |
|---|---|
| `Class.forName("com.foo.Bar")` | Class name from string literal |
| `clazz.getMethod("name", ...)` | Method name + parameter types |
| `clazz.getDeclaredMethod(...)` | Method name + parameter types |
| `clazz.getField("name")` | Field name |
| `clazz.getDeclaredField(...)` | Field name |
| `clazz.getConstructor(...)` | Constructor parameter types |
| `clazz.getDeclaredConstructor(...)` | Constructor parameter types |
| `clazz.newInstance()` | Default constructor |
| `constructor.newInstance(...)` | Specific constructor |
| `method.invoke(...)` | Method invocation target |
| `field.get(...)` / `field.set(...)` | Field access |
| `ServiceLoader.load(SomeInterface.class)` | Interface + all implementations in META-INF/services |
| `Proxy.newProxyInstance(...)` | Interface list for dynamic proxy |
| `MethodHandles.lookup()` | Method handle access |

**Annotation-driven reflection (depth 0, structural):**

| Pattern | What to extract |
|---|---|
| `@JsonProperty`, `@JsonCreator`, etc. | Jackson serialization targets |
| `@XmlElement`, `@XmlRootElement`, etc. | JAXB binding targets |
| `@Entity`, `@Table`, etc. | JPA/Hibernate entities |
| NiFi `@OnScheduled`, `@OnStopped`, etc. | Lifecycle methods invoked via `ReflectionUtils` |
| `implements Serializable` | Serialization config |

**Resource loading (depth 0):**

| Pattern | What to extract |
|---|---|
| `getClass().getResourceAsStream("/...")` | Resource path |
| `ClassLoader.getResource(...)` | Resource path |
| `Thread.currentThread().getContextClassLoader().getResource(...)` | Resource path |
| `Class.getResource(...)` | Resource path |

### Deep transitive analysis

This is the key differentiator. Most reflection doesn't happen in your code — it happens three or four calls deep in a library.

**Example: NiFi PutS3Object -> AWS SDK -> Jackson -> reflection**

```
PutS3Object.onTrigger()
  -> S3Client.putObject(request)
    -> AwsJsonProtocol.serialize(request)
      -> ObjectMapper.writeValueAsString(request)
        -> Class.forName("com.amazonaws.services.s3.model.PutObjectRequest")
        -> clazz.getDeclaredMethods()  // Jackson introspecting for @JsonProperty
```

Static analysis traces this full chain:

1. **Start at entry points** — for NiFi: `onTrigger()`, `@OnScheduled`, `@OnStopped`, etc. For zinc-flow: `main()`, Javalin route handlers, pipeline processor functions.

2. **Build a call graph** — for each entry point, follow every method call. When a call crosses into a dependency JAR, continue following into that JAR's source.

3. **At each node**, check if the method contains a reflection pattern from the table above. If yes, record:
   - The reflected class/method/field
   - The full call chain that leads to it
   - Whether the argument is a static string literal (resolvable) or dynamic (unresolvable)

4. **Mark unresolvable sites** — if the argument to `Class.forName` is a variable computed at runtime (e.g., read from a config file), mark it as unresolvable. These are the YELLOW/RED cases that need the tracing agent as fallback.

5. **Follow ServiceLoader chains** — when `ServiceLoader.load(X.class)` is found, scan `META-INF/services/X` in all JARs on the classpath to find all registered implementations. Each implementation becomes a reflected class.

### Resolution categories

For each reflection call site, the analyzer categorizes it:

| Category | Meaning | Action |
|---|---|---|
| **RESOLVED** | String literal or statically traceable argument — we know exactly what class/method | Generate reflect-config entry directly |
| **ENUM-BOUNDED** | Argument comes from a finite set (enum, switch, if-chain) | Generate entries for all possible values |
| **CONFIG-DRIVEN** | Argument comes from a properties file or environment variable | Scan config files for values, generate entries |
| **DYNAMIC** | Argument is truly computed at runtime (user input, network, etc.) | Flag for tracing agent fallback |

Most reflection in well-written libraries falls into RESOLVED or ENUM-BOUNDED. The DYNAMIC cases are what make the RED processors RED.

## Automated Test Generation

This is the second output of the reflection graph. If we know every code path that leads to reflection, we can generate tests that exercise exactly those paths.

### Why this matters

For GraalVM: a test that exercises a reflection path **validates** that our generated config is correct. If the test passes on native-image, the config is good. If it fails, we missed something.

For general testing: the reflection graph is also a **code coverage map** of the most fragile parts of the code — the parts that use runtime behavior GraalVM can't see. Testing these paths is valuable independent of native-image.

### Test generation strategy

For each reflection call chain, generate a test that:

1. **Sets up the entry point** — instantiate the processor or start the HTTP handler
2. **Provides input that triggers the specific code path** — the right properties, the right FlowFile attributes, the right HTTP request
3. **Asserts the reflection succeeds** — no `ClassNotFoundException`, no `NoSuchMethodException`
4. **Asserts the output is correct** — FlowFile transferred to the right relationship, HTTP response is valid

### For NiFi processors

The analyzer knows:
- Which processor properties trigger which code paths (by following `context.getProperty(X).getValue()` into conditionals)
- Which FlowFile attributes are read (by following `flowFile.getAttribute("X")`)
- Which relationships are possible (from `getRelationships()`)

So it generates tests using NiFi's existing `nifi-mock` TestRunner:

```java
// AUTO-GENERATED: Tests reflection path
//   PutS3Object.onTrigger() -> S3Client.putObject() -> Jackson serialize
// Reflected classes: PutObjectRequest, ObjectMapper, JsonFactory
@Test
void testPutS3Object_jacksonSerializationPath() {
    TestRunner runner = TestRunners.newTestRunner(new PutS3Object());
    runner.setProperty("Bucket", "test-bucket");
    runner.setProperty("Region", "us-east-1");
    runner.setProperty("Access Key", "test");
    runner.setProperty("Secret Key", "test");
    runner.enqueue("test data".getBytes(), Map.of("filename", "test.txt"));
    runner.run();
    // Reflection path was exercised — even if S3 call fails,
    // serialization and class loading happened
}
```

### For zinc-flow

The analyzer knows:
- Which HTTP routes exist (from Javalin handler registration)
- Which pipeline processors are registered
- What each processor does with the FlowFile

So it generates HTTP-based tests:

```java
// AUTO-GENERATED: Tests reflection path
//   Main.main() -> Javalin.start() -> Jetty ServerConnector init
// Reflected classes: ServerConnector, HttpConnectionFactory, SslContextFactory
@Test
void testJavalinStartup_jettyReflection() {
    // Start zinc-flow
    HttpClient client = HttpClient.newHttpClient();
    HttpResponse<String> resp = client.send(
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/flow"))
            .POST(BodyPublishers.ofString("test payload"))
            .build(),
        BodyHandlers.ofString());
    assertEquals(202, resp.statusCode());
}
```

### Test output structure

```
generated-tests/
  nifi/
    PutS3ObjectReflectionTest.java      # one test class per processor
    FetchS3ObjectReflectionTest.java
    ReplaceTextReflectionTest.java
    ...
  zinc-flow/
    JavalinReflectionTest.java          # HTTP endpoint tests
    PipelineReflectionTest.java         # pipeline processor tests
  reports/
    reflection-graph.json               # full graph for inspection
    compatibility-report.md             # GREEN/YELLOW/RED per module
    unresolved-sites.md                 # DYNAMIC sites needing tracing agent
```

## Scanning Targets

### zinc-flow

| Source | What to scan |
|---|---|
| `src/*.java` (transpiled) | Entry points: `main()`, route handlers, processor functions |
| Javalin 6.6.0 source | Jetty init, handler dispatch, JSON serialization |
| Logback 1.5.6 source | XML config parsing, appender loading |
| Jackson (if used via Javalin) | Serializer/deserializer introspection |

Expected result: small reflection surface, mostly RESOLVED. zinc-flow should be straightforward to native-image.

### NiFi processors

For each processor bundle, scan:

| Source | What to scan |
|---|---|
| Processor source | `onTrigger()`, lifecycle methods, property access patterns |
| Direct dependencies | SDK clients, protocol libraries, serialization frameworks |
| Transitive dependencies | Follow to arbitrary depth until no more reflection found |

## NiFi Processor Compatibility Matrix

After excluding processors we don't need (scripting, groovyx, airtable, workday, gcp, azure, couchbase, snowflake, salesforce, slack, jolt), here is the expected compatibility based on known reflection patterns:

### GREEN — no reflection or fully RESOLVED (~8 processors)

| Processor bundle | Why |
|---|---|
| nifi-cipher | Pure crypto (Jagged x25519), no reflection |
| nifi-compress | Commons Compress, JNI libs (zstd, brotli, snappy) — resource bundling only |
| nifi-avro | Apache Avro parsing, minimal reflection — all RESOLVED |
| nifi-geohash | Pure computation |
| nifi-evtx | Windows Event Log parsing, pure data parsing |
| nifi-hl7 | HL7 message parsing |
| nifi-media | Media type detection |
| nifi-stateful-analysis | Statistical analysis |

### YELLOW — reflection exists but expected RESOLVED or ENUM-BOUNDED (~15 processors)

**High confidence** (well-known library reflection patterns):

| Processor bundle | Key dependency | Expected resolution |
|---|---|---|
| nifi-aws | AWS SDK v2 | RESOLVED — SDK uses known annotation-driven serialization |
| nifi-kafka | Kafka Client | RESOLVED — config-driven class loading with known defaults |
| nifi-elasticsearch | REST client + Jackson | RESOLVED — Jackson annotation scanning is structural |
| nifi-mongodb | MongoDB Driver Sync | RESOLVED — BSON codec uses known type set |
| nifi-amqp | RabbitMQ Client | RESOLVED — connection factory reflection is static |
| nifi-mqtt | Paho + HiveMQ (Netty) | RESOLVED — Netty channel init is structural |
| nifi-redis | Jedis/Lettuce | RESOLVED — command set is finite |
| nifi-snmp | SNMP4J | RESOLVED — pure Java, minimal reflection |
| nifi-opentelemetry | Protobuf + Netty | RESOLVED — protobuf is generated code |

**Medium confidence** (need analysis to confirm):

| Processor bundle | Key dependency | Notes |
|---|---|---|
| nifi-graph | Graph DB client | Depends on backing DB — Neo4j is RESOLVED |
| nifi-smb | SMBJ protocol library | Likely RESOLVED — protocol lib |
| nifi-websocket | Jetty WebSocket | RESOLVED — Jetty reflection is well-mapped |
| nifi-box | OkHttp + Jackson | RESOLVED |
| nifi-dropbox | OkHttp + Jackson | RESOLVED |
| nifi-hubspot | OkHttp + Jackson | RESOLVED |

**nifi-standard** (mixed — per sub-processor):

| Sub-processor area | Expected resolution | Key deps |
|---|---|---|
| Text processing (ReplaceText, SplitText, RouteOnAttribute) | GREEN | Pure logic, no reflection |
| Content manipulation (MergeContent, SplitContent) | GREEN | Pure logic |
| HTTP (HandleHttpRequest/Response) | RESOLVED | Jetty — structural reflection |
| TCP/UDP (ListenTCP, ListenUDP, PutTCP) | RESOLVED | Netty — structural reflection |
| FTP/SFTP | RESOLVED | Apache SSHD — algorithm registration |
| XML transforms (TransformXml) | RESOLVED | Saxon-HE — transformer factory |
| Database (ExecuteSQL, PutDatabaseRecord) | ENUM-BOUNDED | JDBC — driver set is finite per deployment |
| DatabaseAdapterDescriptor | CONFIG-DRIVEN | ServiceLoader — scan META-INF/services |

### RED — contains DYNAMIC reflection (~5 processors)

| Processor bundle | Blocker | Why it's DYNAMIC |
|---|---|---|
| nifi-jms | Spring JMS + JNDI | JNDI lookups resolve class names from external naming service at runtime — truly unknowable statically |
| nifi-email | Spring Integration Mail | Spring bean wiring reads class names from XML/annotation context — deeply dynamic |
| nifi-hadoop (HDFS) | Hadoop stack | `Configuration.getClass()` loads classes named in `core-site.xml` / `hdfs-site.xml` — config-driven but config is deployment-specific |
| nifi-parquet | Hadoop dependency chain | Same Hadoop Configuration issue transitively |
| nifi-cdc-mysql | Debezium connector | Limited `Class.forName` — may be ENUM-BOUNDED after analysis, evaluate case-by-case |

### Possible fix paths for RED processors

| Processor | Fix | Effort |
|---|---|---|
| nifi-jms | Replace Spring JMS with direct JMS API (ConnectionFactory + Session + Producer/Consumer) | Medium — Spring is convenience, not necessity |
| nifi-email | Replace Spring Integration Mail with direct Jakarta Mail API | Low — Jakarta Mail is simple, no reflection |
| nifi-hadoop | Use WebHDFS REST API instead of Java HDFS client | Medium — HTTP calls instead of RPC, different performance profile |
| nifi-parquet | Use parquet-java standalone (Hadoop-free fork, actively developed) | Low — drop-in replacement for read/write paths |
| nifi-cdc-mysql | Analyze `Class.forName` calls — if args are string literals, reclassify as RESOLVED | Low — likely reclassifiable |

### Summary

| Category | Count | % of working set |
|---|---|---|
| GREEN (no reflection) | ~8 | 28% |
| YELLOW (RESOLVED/ENUM-BOUNDED) | ~15 + standard sub-processors | 55% |
| RED (DYNAMIC — needs refactoring or tracing fallback) | ~5 | 17% |

**~83% of the target processor set** can have GraalVM configs generated purely from static analysis.

## Implementation Plan

### Phase 1: Build the scanner

1. Build `reflect-scan` — a Java tool that parses `.java` source files (using JavaParser, which zinc already bundles) and detects all reflection patterns from the table above
2. Implement string literal resolution — extract the class/method name from the argument
3. Implement basic call graph — follow method calls within a single source tree
4. Test on zinc-flow (small, known surface)

### Phase 2: Deep transitive analysis

5. Extend call graph to cross JAR boundaries — resolve method calls into dependency source JARs (download source JARs from Maven Central or point at local source)
6. Implement transitive chain tracking — record the full path from entry point to reflection site
7. Implement ServiceLoader resolution — scan `META-INF/services` files
8. Implement annotation-driven detection — Jackson, JAXB, NiFi lifecycle annotations
9. Test on one GREEN NiFi processor (nifi-compress) — verify the graph is complete

### Phase 3: Config generation

10. Generate `reflect-config.json` from RESOLVED sites
11. Generate `resource-config.json` from resource loading sites
12. Generate `jni-config.json` from JNI call sites
13. Generate `proxy-config.json` from `Proxy.newProxyInstance` sites
14. Generate `serialization-config.json` from `Serializable` implementations
15. Build zinc-flow as native image using generated configs — validate

### Phase 4: Test generation

16. Generate NiFi processor test harnesses from the reflection graph — one test class per processor, one test method per reflection path
17. Generate zinc-flow test harnesses — HTTP endpoint tests + pipeline processor tests
18. Run generated tests on JVM — validate they exercise the expected paths
19. Run generated tests on native image — validate configs are complete

### Phase 5: NiFi processor rollout

20. Run scanner on all GREEN processors — generate configs + tests, build native, validate
21. Run scanner on YELLOW processors — generate configs + tests, identify any DYNAMIC sites
22. For DYNAMIC sites: fall back to tracing agent for just those specific paths
23. Run scanner on RED processors — produce compatibility report, identify refactoring targets

### Phase 6: RED processor refactoring (if needed)

24. nifi-email: replace Spring Integration with Jakarta Mail
25. nifi-jms: replace Spring JMS with direct JMS API
26. nifi-parquet: evaluate parquet-java standalone fork
27. nifi-hadoop: evaluate WebHDFS REST approach
28. nifi-cdc-mysql: re-evaluate after analysis (likely reclassifiable)

## Tracing Agent: Fallback for Black-Box Code

The tracing agent is still needed for two cases:

1. **DYNAMIC reflection sites** where the class name is truly computed at runtime and cannot be resolved statically
2. **JNI native libraries** where reflection happens in native code invisible to Java source analysis (e.g., zstd-jni, snappy-java, brotli4j)

For these cases, the tracing agent workflow:

```bash
# Run with tracing agent — only for the specific DYNAMIC paths
java -agentlib:native-image-agent=config-merge-dir=./graal-config \
     -jar target.jar

# Exercise just the DYNAMIC paths (the generated test harness tells you which ones)
# Agent captures what static analysis couldn't

# Merge agent output with statically generated configs
# merge-configs.sh handles deduplication
```

Key flag: `config-merge-dir` (not `config-output-dir`) — merges across multiple runs.

The generated compatibility report tells you exactly which paths need the tracing agent, so you don't need to exercise the entire app — just the specific DYNAMIC sites.

## References

- [GraalVM Native Image Configuration docs](https://www.graalvm.org/latest/reference-manual/native-image/metadata/)
- [GraalVM Tracing Agent docs](https://www.graalvm.org/latest/reference-manual/native-image/metadata/AutomaticMetadataCollection/)
- [GraalVM Reachability Metadata repo](https://github.com/oracle/graalvm-reachability-metadata)
- [JavaParser](https://javaparser.org/) — Java source analysis library (bundled in zinc compiler)
- [NiFi Processor API — nifi-api module](https://nifi.apache.org/documentation/nifi-2.0.0/html/developer-guide.html)
- [NiFi Mock Test Framework — nifi-mock module](https://github.com/apache/nifi/tree/main/nifi-mock)
