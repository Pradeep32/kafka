<p align="center">
<picture>
  <source media="(prefers-color-scheme: light)" srcset="docs/images/kafka-logo-readme-light.svg">
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/kafka-logo-readme-dark.svg">
  <img src="docs/images/kafka-logo-readme-light.svg" alt="Kafka Logo" width="50%"> 
</picture>
</p>

[![CI](https://github.com/apache/kafka/actions/workflows/ci.yml/badge.svg?branch=trunk&event=push)](https://github.com/apache/kafka/actions/workflows/ci.yml?query=event%3Apush+branch%3Atrunk)
[![Flaky Test Report](https://github.com/apache/kafka/actions/workflows/generate-reports.yml/badge.svg?branch=trunk&event=schedule)](https://github.com/apache/kafka/actions/workflows/generate-reports.yml?query=event%3Aschedule+branch%3Atrunk)

[**Apache Kafka**](https://kafka.apache.org) is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

## Challenge Submission Summary

This pull request adds a Docker-based Kafka replication challenge setup and custom MirrorMaker 2 logic for safer disaster-recovery replication from a primary Kafka cluster to a standby Kafka cluster.

### Changes Done

- **Custom MirrorMaker 2 truncation detection**: Added `TruncationDetector` to track the next expected offset for each source `TopicPartition`.
- **Fail-fast data-loss protection**: Added `LogTruncationException` so MirrorMaker 2 fails loudly when the source topic's earliest available offset moves beyond the expected offset, which indicates that records were removed by retention before replication.
- **Topic reset handling**: Added `TopicResetHandler` to handle delete-and-recreate topic scenarios by detecting reset-like offset behavior, seeking the affected partitions to the beginning, and clearing truncation-tracking state.
- **MirrorSourceTask integration**: Updated `MirrorSourceTask.poll()` to compare expected offsets with polled offsets, call `beginningOffsets()` when a gap is seen, distinguish topic reset from truncation, and recover from `OffsetOutOfRangeException` where possible.
- **Commit log producer**: Added a Java producer module that generates synthetic JSON events for the `commit-log` topic with fields such as `event_id`, `timestamp`, `op_type`, `key`, and `value`.
- **Docker verification environment**: Added Docker Compose services for primary Kafka, standby Kafka, MirrorMaker 2, topic initialization, and the commit-log producer.
- **Challenge runner**: Added `scripts/run_challenge.sh` to verify normal replication, log truncation detection, and graceful topic reset handling.
- **Build and image support**: Added Dockerfiles and GitHub workflow changes for building the custom producer and enhanced MirrorMaker 2 images.

### Own Logic Used

The custom logic implemented in this PR is the offset-tracking and recovery logic around MirrorMaker 2 replication:

- **Expected offset tracking**: After successfully polling records, the implementation stores `lastRecord.offset() + 1` as the next expected offset per partition.
- **Gap detection**: On a later poll, if the first returned record offset is greater than the expected offset, the implementation checks the broker's earliest available offset for that partition.
- **Startup truncation check**: After seeking to committed offsets, the implementation checks if the earliest available offset is >= the expected offset (and > 0), catching cases where retention deleted messages while MM2 was stopped but no offset gap exists.
- **Truncation decision**: If the earliest available offset is >= the expected offset (and expected > 0), the code treats it as real log truncation and throws `LogTruncationException` to prevent silent data loss.
- **Topic reset decision**: If the earliest offset is `0` while the expected offset was greater than `0`, the code treats it as a likely topic delete-and-recreate/reset case and clears partition tracking instead of failing.
- **Position-based reset detection**: After polling, the code checks if the consumer's position has been reset to `0` while an expected offset > 0 was tracked — this catches topic recreation during MM2 pause/unpause where the consumer handles the topic ID change internally without throwing `OffsetOutOfRangeException`.
- **OffsetOutOfRange recovery**: When `OffsetOutOfRangeException` is thrown during polling, the code seeks affected partitions to the beginning and resets local truncation tracking so replication can continue from the recreated topic.

This logic is intentionally separate from the default MirrorMaker 2 behavior because the challenge requires explicit handling for both silent truncation risk and topic reset recovery.

### Impact of the Changes

- **Improved data-loss visibility**: Replication no longer silently skips records that disappeared due to Kafka retention before being replicated.
- **Safer DR replication**: The connector fails fast on genuine truncation so operators can investigate instead of assuming the standby cluster is complete.
- **Graceful topic reset recovery**: Delete-and-recreate scenarios are handled by seeking to the beginning of affected partitions and continuing replication from the recreated topic.
- **Operational observability**: Logs now include clear messages for truncation detection, topic reset detection, recovery attempts, and recovery completion.
- **Repeatable validation**: The Docker Compose setup and `run_challenge.sh` script provide repeatable verification of normal replication, truncation handling, and topic reset handling.

### Result Verification

Run the challenge verification with:

```bash
bash scripts/run_challenge.sh
```

The script validates these scenarios:

- **Scenario 1**: Normal replication of `1000` records from `commit-log` to `primary.commit-log`.
- **Scenario 2**: Log truncation detection after retention removes older records before MirrorMaker 2 starts.
- **Scenario 3**: Graceful recovery after deleting and recreating the source topic.

#### Verification Results (GitHub Actions Run)

The script was executed via GitHub Actions on an `ubuntu-latest` runner (Docker preinstalled).

**Run URL**: [GitHub Actions Run #26412062130](https://github.com/Pradeep32/kafka/actions/runs/26412062130)

**Results**:

| Scenario | Status | Details |
|----------|--------|---------|
| Scenario 1: Normal Replication | ✅ PASSED | 1000/1000 messages replicated to standby |
| Scenario 2: Truncation Detection | ✅ PASSED | Truncation detected at startup via earliest offset check, MM2 fails fast with LogTruncationException |
| Scenario 3: Topic Reset Handling | ✅ PASSED | Topic reset detected in initializeConsumer via isTopicReset check, replication resumes from beginning (200→500) |

**Key output**:

```text
[PASS]  17:21:51 All 1000 messages replicated to standby cluster (expected: >=1000, got: 1000)
[PASS]  17:25:32 Log truncation occurred on primary (earliest offset > 0)
[PASS]  17:25:32 MM2 logged truncation detection (LOG TRUNCATION DETECTED or LogTruncationException)
[PASS]  17:27:50 MM2 detected topic reset (TOPIC RESET DETECTED in logs)
[PASS]  17:28:07 Replication resumed after topic reset (200 -> 500 messages)
============================================================
  ALL SCENARIOS COMPLETE
  Assertions: 5 passed, 0 failed
[PASS]  ALL SCENARIOS PASSED SUCCESSFULLY.
```

Full output log: [`docs/images/run_challenge_output.log`](docs/images/run_challenge_output.log)

### Feedback Response & Robustness Improvements

The following improvements were made in response to code review feedback:

| Feedback | Fix Applied |
|----------|-------------|
| **Cold state misclassification**: `getExpectedOffset()` returns -1 until first batch, causing fall-through to topic reset for real truncation | Added `committedOffset > 0` guard in `initializeConsumer()` — only runs topic reset check when committed offsets exist |
| **Truncation vs reset ambiguity**: If all segments are deleted (extreme truncation), `earliest=0` looks like a topic reset | `isTopicReset()` now requires `endOffset > 0` — empty topics (end=0) are NOT treated as reset, avoiding silent data loss |
| **Offset store not cleared**: `TopicResetHandler` only clears in-memory state, not Connect's offset store | Documented natural recovery: post-reset records from offset 0 are committed via `commitRecord()`, updating the offset store. On restart, `initializeConsumer()` re-detects reset if crash occurred before first commit |
| **Expected offsets not seeded on startup**: `getExpectedOffset()` returns -1 until first poll | `initializeConsumer()` seeds `truncationDetector.updateExpectedOffset()` for all committed partitions |
| **JAR merge fragility**: `jar uf` into stock JAR is error-prone | Build step extracts Kafka 4.0.0 JARs from Docker image, compiles only enhanced classes against them, and merges into a copy of the original JAR |

**Detection decision matrix in `initializeConsumer()`:**

| earliest | end | committed | Action |
|----------|-----|-----------|--------|
| > 0, ≥ expected | any | > 0 | **Truncation** → `LogTruncationException` (fail-fast) |
| = 0 | > 0 | > 0 | **Topic reset** → seek to beginning, log `TOPIC RESET DETECTED` |
| = 0 | = 0 | > 0 | **Ambiguous** → NOT treated as reset (could be extreme truncation) |
| any | any | ≤ 0 | **Cold start** → skip checks, use `auto.offset.reset=earliest` |

To re-run verification:

```bash
bash scripts/run_challenge.sh
```

Or trigger the GitHub Actions workflow:

```bash
gh workflow run run-challenge.yml --ref pr-1
```

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test Apache Kafka with Java versions 17 and 25. The `release` parameter in javac is set to `11` for the clients 
and streams modules, and `17` for the rest, ensuring compatibility with their respective
minimum Java versions. Similarly, the `release` parameter in scalac is set to `11` for the streams modules and `17`
for the rest.

Scala 2.13 is the only supported version in Apache Kafka.

### Build a JAR and run it
```bash
./gradlew jar
```

Follow instructions in https://kafka.apache.org/quickstart

### Build source JAR
```bash
./gradlew srcJar
```

### Build aggregated javadoc
```bash
./gradlew aggregatedJavadoc --no-parallel
```

### Build javadoc and scaladoc
```bash
./gradlew javadoc
./gradlew javadocJar # builds a javadoc jar for each module
./gradlew scaladoc
./gradlew scaladocJar # builds a scaladoc jar for each module
./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module
```

### Run unit/integration tests
```bash
./gradlew test  # runs both unit and integration tests
./gradlew unitTest
./gradlew integrationTest
./gradlew test -Pkafka.test.run.flaky=true  # runs tests that are marked as flaky
```

### Force re-running tests without code change
```bash
./gradlew test --rerun-tasks
./gradlew unitTest --rerun-tasks
./gradlew integrationTest --rerun-tasks
```

### Running a particular unit/integration test
```bash
./gradlew clients:test --tests RequestResponseTest
./gradlew streams:integration-tests:test --tests RestoreIntegrationTest
```

### Running a particular unit/integration test N times
```bash
N=500; I=0; while [ $I -lt $N ] && ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done
```

### Running a particular test method within a unit/integration test
```bash
./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate
./gradlew streams:integration-tests:test --tests org.apache.kafka.streams.integration.RestoreIntegrationTest.shouldRestoreNullRecord
```

### Running a particular unit/integration test with log4j output
By default, there will be only a small number of logs output while testing. You can adjust it by changing the `log4j2.yaml` file in the module's `src/test/resources` directory.

For example, if you want to see more logs for clients project tests, you can modify [the line](https://github.com/apache/kafka/blob/trunk/clients/src/test/resources/log4j2.yaml#L35) in `clients/src/test/resources/log4j2.yaml` 
to `level: INFO` and then run:

```bash
./gradlew cleanTest clients:test --tests NetworkClientTest
```

And you should see `INFO` level logs in the file under the `clients/build/test-results/test` directory.

### Specifying test retries
Retries are disabled by default, but you can set maxTestRetryFailures and maxTestRetries to enable retries.

The following example declares -PmaxTestRetries=1 and -PmaxTestRetryFailures=3 to enable a failed test to be retried once, with a total retry limit of 3.

```bash
./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=3
```

See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) and [build.yml](.github/workflows/build.yml) for more details.

### Generating test coverage reports
Generate coverage reports for the whole project:

```bash
./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
```

Generate coverage for a single module, i.e.: 

```bash
./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
```

Coverage reports are located within the module's build directory, categorized by module type:

Core Module (:core): `core/build/reports/scoverageTest/index.html`

Other Modules: `<module>/build/reports/jacoco/test/html/index.html`

### Building a binary release gzipped tarball
```bash
./gradlew clean releaseTarGz
```

The release file can be found inside `./core/build/distributions/`.

### Building auto-generated messages
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:

```bash
./gradlew processMessages processTestMessages
```

See [Apache Kafka Message Definitions](clients/src/main/resources/common/message/README.md) for details on Apache Kafka message protocol.

### Running a Kafka broker

Using compiled files:

```bash
KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
./bin/kafka-server-start.sh config/server.properties
```

Using docker image:

```bash
docker run -p 9092:9092 apache/kafka:latest
```

See [docker/README.md](docker/README.md) for detailed information.

### Cleaning the build
```bash
./gradlew clean
```

### Running a task for a specific project
This is for `core`, `examples` and `clients`

```bash
./gradlew core:jar
./gradlew core:test
```

Streams has multiple sub-projects, but you can run all the tests:

```bash
./gradlew :streams:testAll
```

### Listing all gradle tasks
```bash
./gradlew tasks
```

### Building IDE project
*Note: Please ensure that JDK 17 is used when developing Kafka.*

IntelliJ supports Gradle natively, and it will automatically check Java syntax and compatibility for each module, even if
the Java version shown in the `Structure > Project Settings > Modules` may not be the correct one.

When it comes to Eclipse, run:

```bash
./gradlew eclipse
```

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory, and we don't use Gradle's build directory
to avoid known issues with this configuration.

### Publishing the streams quickstart archetype artifact to maven
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

```bash
cd streams/quickstart
mvn deploy
```

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...

### Installing all projects to the local Maven repository

```bash
./gradlew -PskipSigning=true publishToMavenLocal
```

### Installing specific projects to the local Maven repository

```bash
./gradlew -PskipSigning=true :streams:publishToMavenLocal
```

### Building the test JAR
```bash
./gradlew testJar
```

### Running code quality checks
There are two code quality analysis tools that we regularly run, SpotBugs and Checkstyle.

#### Checkstyle
Checkstyle enforces a consistent coding style in Kafka.
You can run Checkstyle using:

```bash
./gradlew checkstyleMain checkstyleTest spotlessCheck
```

The Checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.
For experiments (or regression testing purposes) add `-PcheckstyleVersion=X.y.z` switch (to override project-defined checkstyle version).

#### Spotless
The import order is a part of static check. Please call `spotlessApply` to optimize Java imports before filing a pull request.

```bash
./gradlew spotlessApply
```

#### SpotBugs
SpotBugs uses static analysis to look for bugs in the code.
You can run SpotBugs using:

```bash
./gradlew spotbugsMain spotbugsTest -x test
```

The SpotBugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### JMH microbenchmarks
We use [JMH](https://openjdk.java.net/projects/code-tools/jmh/) to write microbenchmarks that produce reliable results in the JVM.

See [jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md) for details on how to run the microbenchmarks.

### Dependency Analysis

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

```bash
./gradlew allDeps

./gradlew allDepInsight --configuration runtimeClasspath --dependency com.fasterxml.jackson.core:jackson-databind
```

These take the same arguments as the built-in variants.

### Determining if any dependencies could be updated
```bash
./gradlew dependencyUpdates --no-parallel
```

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: maximum number of test processes to start in parallel. Defaults to the number of processors available to the JVM.
* `maxScalacThreads`: maximum number of worker threads for the scalac backend. Defaults to the lowest of `8` and the number of processors
available to the JVM. The value must be between 1 and 16 (inclusive). 
* `ignoreFailures`: ignore test failures from junit
* `showStandardStreams`: shows standard output and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for SpotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.
* `enableTestCoverage`: enables test coverage plugins and tasks, including bytecode enhancement of classes required to track said
coverage. Note that this introduces some overhead when running tests and hence why it's disabled by default (the overhead
varies, but 15-20% is a reasonable estimate).
* `keepAliveMode`: configures the keep-alive mode for the Gradle compilation daemon - reuse improves start-up time. The values should 
be one of `daemon` or `session` (the default is `daemon`). `daemon` keeps the daemon alive until it's explicitly stopped while
`session` keeps it alive until the end of the build session. This currently only affects the Scala compiler, see
https://github.com/gradle/gradle/pull/21034 for a PR that attempts to do the same for the Java compiler.
* `scalaOptimizerMode`: configures the optimizing behavior of the Scala compiler, the value should be one of `none`, `method`, `inline-kafka` or
`inline-scala` (the default is `inline-kafka`). `none` is the Scala compiler default, which only eliminates unreachable code. `method` also
includes method-local optimizations. `inline-kafka` adds inlining of methods within the kafka packages. Finally, `inline-scala` also
includes inlining of methods within the scala library (which avoids lambda allocations for methods like `Option.exists`). `inline-scala` is
only safe if the Scala library version is the same at compile time and runtime. Since we cannot guarantee this for all cases (for example, users
may depend on the kafka jar for integration tests where they may include a scala library with a different version), we don't enable it by
default. See https://www.lightbend.com/blog/scala-inliner-optimizer for more details.

### Upgrading Gradle version

See [gradle/wrapper/README.md](gradle/wrapper/README.md) for instructions on upgrading the Gradle version.

### Running system tests

See [tests/README.md](tests/README.md).

### Using Trogdor for testing

We use Trogdor as a test framework for Apache Kafka. You can use it to run benchmarks and other workloads.

See [trogdor/README.md](trogdor/README.md).

### Running in Vagrant

See [vagrant/README.md](vagrant/README.md).

### Kafka client examples

See [examples/README.md](examples/README.md).

### Contribution

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html
