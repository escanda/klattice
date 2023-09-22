plugins {
    id("java")
    id("io.quarkus") version "3.3.3"
}

repositories {
    mavenCentral()
}

group = "klattice"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

configurations.all {
    resolutionStrategy {
        force("org.antlr:antlr4-runtime:4.9.2")
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.quarkus:quarkus-junit5")

    implementation(project(":core"))
    implementation(project(":wire"))

    implementation(platform("io.quarkus.platform:quarkus-bom:3.3.3"))

    val immutablesVersion = "2.9.3"
    annotationProcessor("org.immutables:value:${immutablesVersion}")
    compileOnly("org.immutables:value:${immutablesVersion}")

    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-grpc")
    implementation("io.quarkus:quarkus-picocli")

    implementation("io.quarkus:quarkus-rest-client-reactive-jackson")

    val calciteVersion = "1.35.0"
    implementation("org.apache.calcite:calcite-core:${calciteVersion}")
    implementation("org.apache.calcite:calcite-kafka:${calciteVersion}")

    val substraitVersion = "0.17.0"
    implementation("io.substrait:core:${substraitVersion}")
    implementation("io.substrait:isthmus:${substraitVersion}")

    implementation("com.google.guava:guava:32.1.2-jre")
}

tasks.test {
    useJUnitPlatform()
}