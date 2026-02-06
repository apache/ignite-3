#!/usr/bin/env bash

retry_docker_login() {
    local registry="$1"
    local username="$2"
    local password="$3"
    local max_attempts=5
    local attempt=1
    local sleep_interval=5

    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt of $max_attempts: docker login $registry"
        if docker login "$registry" --username "$username" --password "$password"; then
            echo "Command succeeded on attempt $attempt"
            return 0
        else
            if [ $attempt -lt $max_attempts ]; then
                echo "Command failed on attempt $attempt. Retrying in $sleep_interval seconds..."
                sleep $sleep_interval
            fi
            attempt=$((attempt + 1))
        fi
    done

    echo "Command failed after $max_attempts attempts"
    return 1
}

retry_docker_login "docker.gridgain.com/dockerhub-proxy/" '%DOCKERPROXY_USERNAME%' '%DOCKERPROXY_PASSWORD%' || exit 1

echo ""
echo "========================================="
echo "Processing Docker images..."
echo "========================================="

REPO_ROOT="${PWD}"

DOCKER_PROXY_PREFIX="docker.gridgain.com/dockerhub-proxy/"
DOCKER_FILES_COUNT=0
DOCKER_REPLACEMENTS=0

DOCKERFILES=$(find "$REPO_ROOT" -name "Dockerfile*" -type f \
    -not -path "*/.git/*" \
    -not -path "*/.gradle/*" \
    -not -path "*/build/*" \
    -not -path "*/out/*" \
    -not -path "*/.idea/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/.run/*" \
    2>/dev/null || true)

if [ -z "$DOCKERFILES" ]; then
    echo "No Dockerfiles found"
else
    echo "Found Dockerfiles:"
    echo "$DOCKERFILES" | while IFS= read -r dockerfile; do
        echo "  $dockerfile"
    done

    while IFS= read -r dockerfile; do
        if [ ! -f "$dockerfile" ]; then
            continue
        fi

        echo ""
        echo "Processing: $dockerfile"
        DOCKER_FILES_COUNT=$((DOCKER_FILES_COUNT + 1))
        REPLACEMENT_COUNT=0

        TEMP_FILE=$(mktemp)

        while IFS= read -r line || [ -n "$line" ]; do
            if echo "$line" | grep -qE "^\s*FROM\s+"; then
                # Skip if already has proxy prefix
                if echo "$line" | grep -qF "$DOCKER_PROXY_PREFIX"; then
                    echo "$line" >> "$TEMP_FILE"
                    continue
                fi
                # Skip special cases: scratch, --platform
                if echo "$line" | grep -qE "(scratch|--platform)"; then
                    echo "$line" >> "$TEMP_FILE"
                    continue
                fi
                # Replace: FROM image -> FROM docker.proxy.example/dockerhub-proxy/image
                # Handle: FROM image:tag, FROM image@digest, FROM image AS alias
                NEW_LINE=$(echo "$line" | sed -E "s|^(\s*FROM\s+)([^[:space:]]+)(.*)$|\1${DOCKER_PROXY_PREFIX}\2\3|")
                echo "$NEW_LINE" >> "$TEMP_FILE"
                REPLACEMENT_COUNT=$((REPLACEMENT_COUNT + 1))
            else
                # Keep other lines as-is
                echo "$line" >> "$TEMP_FILE"
            fi
        done < "$dockerfile"

        if [ $REPLACEMENT_COUNT -gt 0 ]; then
            mv "$TEMP_FILE" "$dockerfile"
            DOCKER_REPLACEMENTS=$((DOCKER_REPLACEMENTS + REPLACEMENT_COUNT))
            echo "  Updated with $REPLACEMENT_COUNT replacement(s)"
        else
            rm -f "$TEMP_FILE"
            echo "  No changes needed"
        fi
    done <<< "$DOCKERFILES"
fi

echo ""
echo "========================================="
echo "Summary:"
echo "  Dockerfiles processed: $DOCKER_FILES_COUNT"
echo "  Docker replacements: $DOCKER_REPLACEMENTS"
echo "========================================="
echo "Done!"
