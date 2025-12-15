#!/usr/bin/env bash
set +e

NEXUS_API_URL="https://nexus.gridgain.com/service/rest/v1/repositories"

REPO_ROOT="${PWD}"

#set +x
echo "Fetching proxy repositories from Nexus..."
echo "REPO_ROOT is set to: $REPO_ROOT"

# Fetch repositories from Nexus
REPOS_JSON=$(curl -s -X GET "$NEXUS_API_URL" 2>&1)

if [ $? -ne 0 ] || [ -z "$REPOS_JSON" ]; then
    echo "Warning: Failed to fetch repositories from Nexus. Continuing without proxy replacements." >&2
    exit 0
fi

TEMP_MAP=$(mktemp 2>/dev/null)
if [ -z "$TEMP_MAP" ]; then
    echo "Warning: Failed to create temporary file. Continuing without proxy replacements." >&2
    exit 0
fi
trap "rm -f $TEMP_MAP" EXIT

echo "$REPOS_JSON" | jq -r '.[] | select(.type == "proxy") | "\(.attributes.proxy.remoteUrl)|\(.url)"' > "$TEMP_MAP" 2>/dev/null

if [ ! -s "$TEMP_MAP" ]; then
    echo "Error: No proxy repositories found or failed to parse response" >&2
    echo "Response was:"
    echo "$REPOS_JSON" | head -20
    exit 0
fi

echo "Found proxy repositories:"
cat "$TEMP_MAP" | while IFS='|' read -r remote_url nexus_url; do
    echo "  $remote_url -> $nexus_url"
done

TOTAL_FILES=0
TOTAL_REPLACEMENTS=0

while IFS='|' read -r remote_url nexus_url; do
    [ -z "$remote_url" ] && continue

    echo ""
    echo "Replacing: $remote_url -> $nexus_url"

    # Search for URL with or without trailing slash
    # Try both versions to find files
    SEARCH_URL="${remote_url%/}"
    FILES_WITH_SLASH=$(find "$REPO_ROOT" \( -name "build.gradle" -o -name "settings.gradle" -o -name "gradle-wrapper.properties" \) -type f \
        -not -path "*/.git/*" \
        -not -path "*/.gradle/*" \
        -not -path "*/build/*" \
        -not -path "*/out/*" \
        -not -path "*/.idea/*" \
        -not -path "*/node_modules/*" \
        -not -path "*/.run/*" \
        -exec grep -lF "$remote_url" {} \; 2>/dev/null || true)
    FILES_WITHOUT_SLASH=$(find "$REPO_ROOT" \( -name "build.gradle" -o -name "settings.gradle" -o -name "gradle-wrapper.properties" \) -type f \
        -not -path "*/.git/*" \
        -not -path "*/.gradle/*" \
        -not -path "*/build/*" \
        -not -path "*/out/*" \
        -not -path "*/.idea/*" \
        -not -path "*/node_modules/*" \
        -not -path "*/.run/*" \
        -exec grep -lF "$SEARCH_URL" {} \; 2>/dev/null || true)
    FILES_TO_UPDATE=$(echo -e "${FILES_WITH_SLASH}\n${FILES_WITHOUT_SLASH}" 2>/dev/null | grep -v '^$' 2>/dev/null | sort -u 2>/dev/null || true)

    if [ -z "$FILES_TO_UPDATE" ]; then
        echo "  No files found containing this URL"
        echo "  Debug: Searched for '$remote_url' and '${SEARCH_URL}'"
        continue
    else
        echo "  Found $(echo "$FILES_TO_UPDATE" | wc -l | tr -d ' ') file(s) to process"
    fi

    while IFS= read -r file; do
        if [ -f "$file" ]; then
            TOTAL_FILES=$((TOTAL_FILES + 1))
            
            # Count occurrences - check which format exists in the file
            COUNT_WITH_SLASH=0
            COUNT_WITHOUT_SLASH=0
            if grep -qF "$remote_url" "$file" 2>/dev/null; then
                COUNT_WITH_SLASH=$(grep -oF "$remote_url" "$file" 2>/dev/null | wc -l | tr -d ' ' || echo "0")
            fi
            if grep -qF "$SEARCH_URL" "$file" 2>/dev/null; then
                COUNT_WITHOUT_SLASH=$(grep -oF "$SEARCH_URL" "$file" 2>/dev/null | wc -l | tr -d ' ' || echo "0")
            fi
            
            # Use the count that's greater than 0, or prefer the one with slash if both exist
            if [ "${COUNT_WITH_SLASH:-0}" -gt 0 ]; then
                COUNT=$COUNT_WITH_SLASH
                USE_SLASH_VERSION=true
            elif [ "${COUNT_WITHOUT_SLASH:-0}" -gt 0 ]; then
                COUNT=$COUNT_WITHOUT_SLASH
                USE_SLASH_VERSION=false
            else
                COUNT=0
                USE_SLASH_VERSION=false
            fi
            
            TOTAL_REPLACEMENTS=$((TOTAL_REPLACEMENTS + ${COUNT:-0})) 2>/dev/null || TOTAL_REPLACEMENTS=$TOTAL_REPLACEMENTS

            if [ "${COUNT:-0}" -gt 0 ]; then
                echo "  Processing: $file (found $COUNT occurrence(s))"

                # Escape special regex characters for sed (using | as delimiter)
                # Determine which format to replace based on what's in the file
                if [ "$USE_SLASH_VERSION" = "true" ]; then
                    # File has URL with trailing slash - replace that version
                    ESCAPED_REMOTE=$(printf '%s\n' "$remote_url" | sed 's/\./\\./g; s/\*/\\*/g; s/\^/\\^/g; s/\$/\\$/g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g; s/|/\\|/g; s/\\/\\\\/g')
                    REPLACE_URL="$nexus_url"
                else
                    # File has URL without trailing slash - replace that version
                    ESCAPED_REMOTE=$(printf '%s\n' "$SEARCH_URL" | sed 's/\./\\./g; s/\*/\\*/g; s/\^/\\^/g; s/\$/\\$/g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g; s/|/\\|/g; s/\\/\\\\/g')
                    # Use nexus_url without trailing slash to match file format
                    REPLACE_URL="${nexus_url%/}"
                fi
                ESCAPED_NEXUS=$(printf '%s\n' "$REPLACE_URL" | sed 's/\./\\./g; s/\*/\\*/g; s/\^/\\^/g; s/\$/\\$/g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g; s/|/\\|/g; s/\\/\\\\/g')

                # Debug: show what we're replacing
                echo "    Replacing: $remote_url -> $nexus_url"

                if [[ "$OSTYPE" == "darwin"* ]]; then
                    # macOS sed
                    sed -i '' "s|${ESCAPED_REMOTE}|${ESCAPED_NEXUS}|g" "$file" 2>/dev/null || true
                else
                    # Linux sed
                    sed -i "s|${ESCAPED_REMOTE}|${ESCAPED_NEXUS}|g" "$file" 2>/dev/null || true
                fi
                
                # Verify replacement worked
                if grep -qF "$REPLACE_URL" "$file" 2>/dev/null; then
                    echo "    ✓ Replacement successful"
                else
                    echo "    ⚠ Warning: Replacement may have failed - URL not found after replacement"
                fi
            else
                echo "  Skipping: $file (no matches found)"
            fi
        fi
    done <<< "$FILES_TO_UPDATE"

done < "$TEMP_MAP"

echo ""
echo "========================================="
echo "Processing GitHub URLs in dependencies.cmake files..."
echo "========================================="

GITHUB_PROXY_URL=$(echo "$REPOS_JSON" | jq -r '.[] | select(.name == "github-raw") | .url' | head -1)

if [ -z "$GITHUB_PROXY_URL" ] || [ "$GITHUB_PROXY_URL" = "null" ]; then
    echo "Warning: GitHub proxy repository not found in Nexus. Skipping GitHub URL replacements."
    GITHUB_PROXY_PREFIX=""
else
    GITHUB_PROXY_PREFIX="${GITHUB_PROXY_URL%/}/"
    echo "Using GitHub proxy: $GITHUB_PROXY_PREFIX"
fi

GITHUB_URL_PREFIX="https://github.com/"
CMAKE_FILES_COUNT=0
CMAKE_REPLACEMENTS=0

CMAKE_FILES=$(find "$REPO_ROOT" -name "dependencies.cmake" -type f \
    -not -path "*/.git/*" \
    -not -path "*/.gradle/*" \
    -not -path "*/build/*" \
    -not -path "*/out/*" \
    -not -path "*/.idea/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/.run/*" \
    2>/dev/null || true)

if [ -z "$GITHUB_PROXY_PREFIX" ]; then
    echo "Skipping GitHub URL replacements (proxy not configured)"
elif [ -z "$CMAKE_FILES" ]; then
    echo "No dependencies.cmake files found"
else
    echo "Found dependencies.cmake files:"
    echo "$CMAKE_FILES" | while IFS= read -r cmake_file; do
        echo "  $cmake_file"
    done

    while IFS= read -r cmake_file; do
        if [ ! -f "$cmake_file" ]; then
            continue
        fi

        echo ""
        echo "Processing: $cmake_file"
        CMAKE_FILES_COUNT=$((CMAKE_FILES_COUNT + 1))
        REPLACEMENT_COUNT=0

        TEMP_FILE=$(mktemp)

        while IFS= read -r line || [ -n "$line" ]; do
            if echo "$line" | grep -qF "$GITHUB_URL_PREFIX"; then
                if echo "$line" | grep -qF "$GITHUB_PROXY_PREFIX"; then
                    echo "$line" >> "$TEMP_FILE"
                    continue
                fi
                NEW_LINE=$(echo "$line" | sed "s|${GITHUB_URL_PREFIX}|${GITHUB_PROXY_PREFIX}|g")
                echo "$NEW_LINE" >> "$TEMP_FILE"
                REPLACEMENT_COUNT=$((REPLACEMENT_COUNT + 1))
            else
                echo "$line" >> "$TEMP_FILE"
            fi
        done < "$cmake_file"

        if [ $REPLACEMENT_COUNT -gt 0 ]; then
            mv "$TEMP_FILE" "$cmake_file"
            CMAKE_REPLACEMENTS=$((CMAKE_REPLACEMENTS + REPLACEMENT_COUNT))
            echo "  Updated with $REPLACEMENT_COUNT replacement(s)"
        else
            rm -f "$TEMP_FILE"
            echo "  No changes needed"
        fi
    done <<< "$CMAKE_FILES"
fi

echo ""
echo "========================================="
echo "Summary:"
echo "  Maven files processed: $TOTAL_FILES"
echo "  Maven replacements: $TOTAL_REPLACEMENTS"
echo "  dependencies.cmake files processed: $CMAKE_FILES_COUNT"
echo "  GitHub URL replacements: $CMAKE_REPLACEMENTS"
echo "========================================="
echo "Done!"
exit 0

