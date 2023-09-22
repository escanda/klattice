plugins {
    id("java")
    id("io.quarkus") version "3.3.3"
}

group = "klattice"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.quarkus:quarkus-junit5")

    implementation(platform("io.quarkus.platform:quarkus-bom:3.3.3"))
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-grpc")
    implementation("io.quarkus:quarkus-resteasy-reactive")
    implementation("io.quarkus:quarkus-rest-client-reactive-jackson")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")

    val substraitVersion = "0.17.0"
    implementation("io.substrait:core:${substraitVersion}")
}

tasks.test {
    useJUnitPlatform()
}