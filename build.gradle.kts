plugins {
    java
}

allprojects {
    group = "com.github.fedomn"
    version = "1.0"
}

subprojects {
    apply(plugin = "java")

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation("org.apache.calcite:calcite-core:1.30.0")
        implementation("org.apache.lucene:lucene-core:8.8.2")
        implementation("org.apache.lucene:lucene-queryparser:8.8.2")
        implementation("au.com.bytecode:opencsv:2.4")
        implementation("au.com.bytecode:opencsv:2.4")
        implementation("org.slf4j:slf4j-api:1.7.36")
        implementation("org.slf4j:slf4j-log4j12:1.7.36")
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    }

    tasks.getByName<Test>("test") {
        useJUnitPlatform()
    }
}
