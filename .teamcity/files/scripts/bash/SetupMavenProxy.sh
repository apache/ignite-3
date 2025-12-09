# Script to replace Maven repository URLs with Nexus proxy URLs
# This helps avoid rate limits by using a local Nexus proxy
NEXUS_API_URL="https://nexus.gridgain.com/service/rest/v1/repositories"

REPO_ROOT="${PWD}"

set +x
echo "Fetching proxy repositories from Nexus..."
echo "REPO_ROOT is set to: $REPO_ROOT"

# Fetch repositories from Nexus
REPOS_JSON=$(curl -s -X GET "$NEXUS_API_URL")

if [ $? -ne 0 ] || [ -z "$REPOS_JSON" ]; then
    echo "Error: Failed to fetch repositories from Nexus" >&2
    exit 0
fi

TEMP_MAP=$(mktemp)
trap "rm -f $TEMP_MAP" EXIT

echo "$REPOS_JSON" | jq -r '.[] | select(.type == "proxy") | "\(.attributes.proxy.remoteUrl)|\(.url)"' > "$TEMP_MAP"

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

    FILES_TO_UPDATE=$(find "$REPO_ROOT" \( -name "build.gradle" -o -name "gradle-wrapper.properties" \) -type f \
        -not -path "*/.git/*" \
        -not -path "*/.gradle/*" \
        -not -path "*/build/*" \
        -not -path "*/out/*" \
        -not -path "*/.idea/*" \
        -not -path "*/node_modules/*" \
        -not -path "*/.run/*" \
        -exec grep -lF "$remote_url" {} \; 2>/dev/null || true)

    if [ -z "$FILES_TO_UPDATE" ]; then
        echo "  No files found containing this URL"
        continue
    fi

    while IFS= read -r file; do
        if [ -f "$file" ]; then
            TOTAL_FILES=$((TOTAL_FILES + 1))
            echo "  Processing: $file"

            # Count occurrences before replacement
            COUNT=$(grep -o "$remote_url" "$file" | wc -l)
            TOTAL_REPLACEMENTS=$((TOTAL_REPLACEMENTS + COUNT))

            # Escape special regex characters for sed (using | as delimiter)
            # Escape each special character individually for safety
            ESCAPED_REMOTE=$(printf '%s\n' "$remote_url" | sed 's/\./\\./g; s/\*/\\*/g; s/\^/\\^/g; s/\$/\\$/g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g; s/|/\\|/g; s/\\/\\\\/g')
            ESCAPED_NEXUS=$(printf '%s\n' "$nexus_url" | sed 's/\./\\./g; s/\*/\\*/g; s/\^/\\^/g; s/\$/\\$/g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g; s/|/\\|/g; s/\\/\\\\/g')

            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS sed
                sed -i '' "s|${ESCAPED_REMOTE}|${ESCAPED_NEXUS}|g" "$file"
            else
                # Linux sed
                sed -i "s|${ESCAPED_REMOTE}|${ESCAPED_NEXUS}|g" "$file"
            fi
        fi
    done <<< "$FILES_TO_UPDATE"

done < "$TEMP_MAP"

echo ""
echo "Summary:"
echo "  Files processed: $TOTAL_FILES"
echo "  Total replacements: $TOTAL_REPLACEMENTS"
echo "Done!"

