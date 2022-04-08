// copied from calcite example csv build.gradle.kts

val classpath by configurations.creating {
    isCanBeConsumed = false
    extendsFrom(configurations.testRuntimeClasspath.get())
}

val jar by tasks.getting(Jar::class) {
    archiveFileName.set("indexer.jar")
    manifest {
        attributes(
            "Main-Class" to "com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer",
            "Class-Path" to provider {
                // Class-Path is a list of URLs
                classpath.joinToString(" ") {
                    it.toURI().toURL().toString()
                }
            }
        )
    }
}