import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

val protobufVersion by extra("3.20.0")
val protobufPluginVersion by extra("0.8.18")
val grpcVersion by extra("1.45.1")
val grpcKotlinVersion by extra("1.2.1")
val kotlinCoroutinesVersion by extra("1.6.0")

plugins {
    kotlin("jvm")
    id("com.google.protobuf") version "0.8.17"
}

java.sourceCompatibility = JavaVersion.VERSION_11

dependencies {

    api(kotlin("stdlib"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")

    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")

    api("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")

    api("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
}

sourceSets {
    val main by getting { }
    main.java.srcDirs("build/generated/source/proto/main/java")
    main.java.srcDirs("build/generated/source/proto/main/grpc")
    main.java.srcDirs("build/generated/source/proto/main/kotlin")
   main.java.srcDirs("build/generated/source/proto/main/grpckt")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${protobufVersion}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:${grpcKotlinVersion}:jdk7@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
            it.builtins {
                id("kotlin")
            }
        }
    }
}

tasks.jar {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
