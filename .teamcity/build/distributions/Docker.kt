package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript

object Docker : BuildType({
    name = "Docker Images"

    artifactRules = """
        ignite:*-amd64.tar.gz => ignite-docker
        ignite:*-arm64.tar.gz => ignite-docker
    """.trimIndent()

    steps {
        customScript(type = "bash") {
            name = "Setup Docker Proxy"
        }
        customGradle {
            name = "Build Docker Image (AMD64)"
            tasks = ":packaging:docker -Ptarget_platform=linux/amd64 -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
        script {
            name = "Save AMD64 Image"
            scriptContent = """
                # Get the image tag (version) by finding the apacheignite/ignite image
                IMAGE_TAG=${'$'}(docker images --format "table {{.Repository}}:{{.Tag}}" | grep '^apacheignite/ignite:' | head -1 | cut -d':' -f2)
                if [ -z "${'$'}IMAGE_TAG" ]; then
                    echo "Error: Could not find apacheignite/ignite image"
                    exit 1
                fi
                docker save apacheignite/ignite:${'$'}{IMAGE_TAG} -o ignite-${'$'}{IMAGE_TAG}-amd64.tar
                gzip -f ignite-${'$'}{IMAGE_TAG}-amd64.tar
            """.trimIndent()
        }
        script {
            name = "Setup Docker for ARM64 Builds"
            scriptContent = """
                #!/bin/bash 

                # docker buildx create --name container --driver docker-container --bootstrap --use
                # docker buildx build --load -t linux/arm64 --builder=container .
                docker run --privileged --rm tonistiigi/binfmt --install all
            """.trimIndent()
        }
        customGradle {
            name = "Build Docker Image (ARM64)"
            tasks = ":packaging:docker -Ptarget_platform=linux/arm64 -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
        script {
            name = "Save ARM64 Image"
            scriptContent = """
                # Get the image tag (version) by finding the apacheignite/ignite image
                IMAGE_TAG=${'$'}(docker images --format "table {{.Repository}}:{{.Tag}}" | grep "apacheignite/ignite:" | head -1 | cut -d':' -f2)
                if [ -z "${'$'}IMAGE_TAG" ]; then
                    echo "Error: Could not find apacheignite/ignite image"
                    exit 1
                fi
                docker save apacheignite/ignite:${'$'}{IMAGE_TAG} -o ignite-${'$'}{IMAGE_TAG}-arm64.tar
                gzip -f ignite-${'$'}{IMAGE_TAG}-arm64.tar
            """.trimIndent()
        }
    }

    failureConditions {
        executionTimeoutMin = 90
    }
})
