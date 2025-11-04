plugins {
  java
  jacoco
}

group = "io.rqlite"
version = "9.1.2.0"

repositories {
  mavenCentral()
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
  }
}

dependencies {
  // Test framework
  testImplementation("io.github.j8spec:j8spec:3.0.0")
  testImplementation("junit:junit:4.13.2")

  // JDK8-compatible test runtime
  testImplementation("com.zaxxer:HikariCP:3.4.5")
  // Removed io.vacco.metolithe and io.vacco.shax (require Java 11+)
  // Removed org.jetbrains.exposed dependencies to avoid Java 11+ requirements
}

sourceSets {
  named("test") {
    java {
      // Compile all tests except generated schema/dao classes (require removed libs)
      exclude("**/schema/**")
      exclude("**/dao/**")
    }
  }
}

tasks.test {
  useJUnit()
  testLogging {
    events("passed", "skipped", "failed")
  }
}

// Keep resource processing to embed project version
tasks.processResources {
  filesMatching("io/rqlite/jdbc/version") {
    expand("projectVersion" to version)
  }
}

// Keep Jacoco report configuration
tasks.withType<JacocoReport> {
  afterEvaluate {
    classDirectories.setFrom(
      files(classDirectories.files.map {
        fileTree(it) {
          exclude("io/rqlite/json/**")
        }
      })
    )
  }
}