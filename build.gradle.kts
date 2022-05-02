plugins {
    kotlin("jvm") version "1.6.20" apply false
    id("org.jlleitschuh.gradle.ktlint") version "10.2.1"
    id("org.jetbrains.kotlinx.kover") version "0.5.0"
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions {
            jvmTarget = "11"
            freeCompilerArgs = listOf("-opt-in=kotlin.RequiresOptIn", "-Xcontext-receivers")
        }
    }
}

ktlint {
    version.set("0.45.2")
}

