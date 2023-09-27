plugins {
    id("java")
    id("io.quarkus") version "3.3.3"
}

group = "klattice"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.quarkus:quarkus-junit5")

    implementation(project(":core"))

    implementation(platform("io.quarkus.platform:quarkus-bom:3.3.3"))
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")

    implementation("io.quarkus:quarkus-container-image-jib")

    implementation("io.quarkus:quarkus-rest-client-reactive")

    implementation("io.quarkus:quarkus-smallrye-reactive-messaging-kafka")
    implementation("io.confluent:kafka-schema-registry-client:7.4.0")
    implementation("io.quarkus:quarkus-confluent-registry-avro")

    implementation("org.apache.commons:commons-csv:1.10.0")

    val parquetVersion = "1.13.1"
    implementation("org.apache.parquet:parquet-avro:${parquetVersion}")
    implementation("org.apache.parquet:parquet-hadoop:${parquetVersion}")

    val hadoopVersion = "3.3.6"
    implementation("org.apache.hadoop:hadoop-common:${hadoopVersion}")
    implementation("org.apache.hadoop:hadoop-client:${hadoopVersion}")
    implementation("org.apache.avro:avro:1.11.2")
    implementation("org.apache.thrift:libthrift:0.18.1")

    implementation("com.google.guava:guava:32.1.2-jre")

    implementation("io.confluent:kafka-avro-serializer:7.2.0") {
        exclude(group="jakarta.ws.rs", module="jakarta.ws.rs-api")
    }
}

tasks.test {
    useJUnitPlatform()
}
