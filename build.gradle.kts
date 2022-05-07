plugins {
    kotlin("jvm") version "1.6.20" apply false
    id("org.jlleitschuh.gradle.ktlint") version "10.2.1"
    id("org.jetbrains.kotlinx.kover") version "0.5.0"
    id("org.sonarqube") version "3.3"
    id("jacoco")
    id("io.gitlab.arturbosch.detekt").version("1.20.0")
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "io.gitlab.arturbosch.detekt")

    detekt {
        source = objects.fileCollection().from(
            io.gitlab.arturbosch.detekt.extensions.DetektExtension.DEFAULT_SRC_DIR_JAVA,
            io.gitlab.arturbosch.detekt.extensions.DetektExtension.DEFAULT_TEST_SRC_DIR_JAVA,
            io.gitlab.arturbosch.detekt.extensions.DetektExtension.DEFAULT_SRC_DIR_KOTLIN,
            io.gitlab.arturbosch.detekt.extensions.DetektExtension.DEFAULT_TEST_SRC_DIR_KOTLIN,
        )
        buildUponDefaultConfig = true
        ignoreFailures = true
        baseline = file("$rootDir/config/detekt/baseline.xml")

        tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
            kotlinOptions {
                jvmTarget = "11"
                freeCompilerArgs = listOf("-opt-in=kotlin.RequiresOptIn", "-Xcontext-receivers")
            }
        }
    }
}

ktlint {
    version.set("0.45.2")
}

sonarqube {
    properties {
        property("sonar.projectKey", "IAAA-Lab_frontier")
        property("sonar.organization", "iaaa-lab")
        property("sonar.host.url", "https://sonarcloud.io")
        property("sonar.verbose", "true")
        property("sonar.kotlin.detekt.reportPaths", "${project.projectDir}/service/build/reports/detekt/detekt.xml")
        property("sonar.coverage.jacoco.xmlReportPaths", "${project.projectDir}/build/reports/kover/report.xml")
    }
}

jacoco {
    toolVersion = "0.8.8"
}

tasks.named("sonarqube") {
    dependsOn(tasks.koverMergedXmlReport)
}

tasks.named("koverMergedXmlReport") {
    dependsOn(tasks.detekt)
    dependsOn(subprojects.map { it.tasks.named("detekt") })
}
