plugins {
    kotlin("jvm")
    id("org.jlleitschuh.gradle.ktlint")
}

val prometheusVersion by extra("0.14.1")
val grpcVersion by extra("1.45.1")

dependencies {
    implementation(project(":api"))
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.6.20")

    implementation("info.picocli:picocli:4.6.3")
    implementation("io.github.microutils:kotlin-logging-jvm:2.1.20")
    implementation("ch.qos.logback:logback-classic:1.2.10")

    implementation("com.sksamuel.hoplite:hoplite-core:2.1.1")

    implementation("io.prometheus:simpleclient:$prometheusVersion")
    implementation("io.prometheus:simpleclient_hotspot:$prometheusVersion")
    implementation("io.prometheus:simpleclient_httpserver:$prometheusVersion")

    runtimeOnly("io.grpc:grpc-netty:1.45.0")

    testImplementation(kotlin("test"))
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
}

tasks.test {
    useJUnitPlatform()
}
