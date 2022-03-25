plugins {
    java
}

group = "com.github.fedomn"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.calcite:calcite-core:1.30.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}