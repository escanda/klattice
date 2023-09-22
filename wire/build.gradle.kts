plugins {
    id("java")
    id("io.quarkus") version "3.3.3"
    id("org.kordamp.gradle.jandex") version "1.1.0"
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

    implementation(project(":core"))

    implementation(platform("io.quarkus.platform:quarkus-bom:3.3.3"))
    implementation("io.quarkus:quarkus-netty")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("io.quarkus:quarkus-grpc")

    implementation("com.google.guava:guava:32.1.2-jre")

    val substraitVersion = "0.17.0"
    implementation("io.substrait:core:${substraitVersion}")
}

tasks.test {
    useJUnitPlatform()
}