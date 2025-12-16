#!/usr/bin/env python3
import json
import os
import re
import sys
from pathlib import Path
from urllib.request import urlopen, Request

CONFIG = {
    'nexus_api_url': 'https://nexus.gridgain.com/service/rest/v1/repositories',
    'repo_root': os.getcwd(),
    'maven_file_patterns': ['build.gradle', 'settings.gradle', 'gradle-wrapper.properties'],
    'cmake_file_pattern': 'dependencies.cmake',
    'exclude_dirs': ['.git', '.gradle', 'build', 'out', '.idea', 'node_modules', '.run'],
    'github_repo_name': 'github-raw',
    'github_url_prefix': 'https://github.com/',
}

def safe_request(url):
    try:
        req = Request(url)
        with urlopen(req, timeout=30) as response:
            return response.read().decode('utf-8')
    except Exception:
        return None

def find_files(root, patterns, exclude_dirs):
    root_path = Path(root)
    files = []
    for pattern in patterns:
        for file_path in root_path.rglob(pattern):
            parts = file_path.parts
            if any(exclude in parts for exclude in exclude_dirs):
                continue
            files.append(file_path)
    return sorted(set(files))

def normalize_url(url):
    return url.rstrip('/')

def escape_regex(text):
    return re.escape(text)

def escape_gradle_url(url):
    return url.replace('://', '\\://')

def unescape_gradle_url(url):
    return url.replace('\\://', '://')

def replace_in_file(file_path, search_url, replace_url, use_slash_version, use_escaped=False):
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content

        if use_escaped:
            search_pattern = escape_gradle_url(search_url if use_slash_version else normalize_url(search_url))
            replace_pattern = escape_gradle_url(replace_url if use_slash_version else normalize_url(replace_url))
        else:
            search_pattern = search_url if use_slash_version else normalize_url(search_url)
            replace_pattern = replace_url if use_slash_version else normalize_url(replace_url)

        escaped_search = escape_regex(search_pattern)
        matches_before = len(re.findall(escaped_search, content))
        content = re.sub(escaped_search, replace_pattern, content)

        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return matches_before
        return 0
    except Exception:
        return 0

def process_maven_replacements(repo_root, maven_file_patterns, exclude_dirs, proxy_mappings):
    total_files = 0
    total_replacements = 0

    files = find_files(repo_root, maven_file_patterns, exclude_dirs)

    for remote_url, nexus_url in proxy_mappings.items():
        if not remote_url:
            continue

        print(f"\nReplacing: {remote_url} -> {nexus_url}")

        matching_files = []
        for file_path in files:
            try:
                content = file_path.read_text(encoding='utf-8')
                is_gradle_wrapper = file_path.name == 'gradle-wrapper.properties'
                escaped_remote = escape_gradle_url(remote_url) if is_gradle_wrapper else None
                escaped_normalized = escape_gradle_url(normalize_url(remote_url)) if is_gradle_wrapper else None

                if remote_url in content or normalize_url(remote_url) in content:
                    matching_files.append(file_path)
                elif is_gradle_wrapper and (escaped_remote and escaped_remote in content or escaped_normalized and escaped_normalized in content):
                    matching_files.append(file_path)
            except Exception:
                continue

        if not matching_files:
            print(f"  No files found containing this URL")
            continue

        print(f"  Found {len(matching_files)} file(s) to process")

        for file_path in matching_files:
            try:
                content = file_path.read_text(encoding='utf-8')
                is_gradle_wrapper = file_path.name == 'gradle-wrapper.properties'

                count_with_slash = content.count(remote_url)
                count_without_slash = content.count(normalize_url(remote_url))
                escaped_remote = escape_gradle_url(remote_url) if is_gradle_wrapper else None
                escaped_normalized = escape_gradle_url(normalize_url(remote_url)) if is_gradle_wrapper else None
                count_escaped_slash = content.count(escaped_remote) if escaped_remote else 0
                count_escaped_no_slash = content.count(escaped_normalized) if escaped_normalized else 0

                if count_with_slash > 0:
                    count = count_with_slash
                    use_slash = True
                    use_escaped = False
                elif count_without_slash > 0:
                    count = count_without_slash
                    use_slash = False
                    use_escaped = False
                elif count_escaped_slash > 0:
                    count = count_escaped_slash
                    use_slash = True
                    use_escaped = True
                elif count_escaped_no_slash > 0:
                    count = count_escaped_no_slash
                    use_slash = False
                    use_escaped = True
                else:
                    count = 0
                    use_slash = False
                    use_escaped = False

                if count > 0:
                    total_files += 1
                    print(f"  Processing: {file_path} (found {count} occurrence(s))")
                    print(f"    Replacing: {remote_url} -> {nexus_url}")

                    replacements = replace_in_file(file_path, remote_url, nexus_url, use_slash, use_escaped)
                    total_replacements += replacements

                    try:
                        new_content = file_path.read_text(encoding='utf-8')
                        escaped_nexus = escape_gradle_url(nexus_url) if is_gradle_wrapper else None
                        escaped_nexus_normalized = escape_gradle_url(normalize_url(nexus_url)) if is_gradle_wrapper else None

                        if nexus_url in new_content or normalize_url(nexus_url) in new_content:
                            print(f"    ✓ Replacement successful")
                        elif is_gradle_wrapper and (escaped_nexus and escaped_nexus in new_content or escaped_nexus_normalized and escaped_nexus_normalized in new_content):
                            print(f"    ✓ Replacement successful")
                        else:
                            print(f"    ⚠ Warning: Replacement may have failed - URL not found after replacement")
                    except Exception:
                        pass
            except Exception:
                continue

    return total_files, total_replacements

def process_github_replacements(repo_root, cmake_file_pattern, exclude_dirs, github_proxy_prefix):
    if not github_proxy_prefix:
        print("Skipping GitHub URL replacements (proxy not configured)")
        return 0, 0

    files = find_files(repo_root, [cmake_file_pattern], exclude_dirs)

    if not files:
        print("No dependencies.cmake files found")
        return 0, 0

    print("Found dependencies.cmake files:")
    for file_path in files:
        print(f"  {file_path}")

    total_files = 0
    total_replacements = 0

    for file_path in files:
        try:
            content = file_path.read_text(encoding='utf-8')
            lines = content.splitlines(keepends=True)
            new_lines = []
            replacement_count = 0

            for line in lines:
                if CONFIG['github_url_prefix'] in line:
                    if github_proxy_prefix in line:
                        new_lines.append(line)
                    else:
                        new_line = line.replace(CONFIG['github_url_prefix'], github_proxy_prefix)
                        new_lines.append(new_line)
                        replacement_count += 1
                else:
                    new_lines.append(line)

            if replacement_count > 0:
                total_files += 1
                total_replacements += replacement_count
                print(f"\nProcessing: {file_path}")
                print(f"  Updated with {replacement_count} replacement(s)")
                file_path.write_text(''.join(new_lines), encoding='utf-8')
            else:
                print(f"\nProcessing: {file_path}")
                print(f"  No changes needed")
        except Exception:
            continue

    return total_files, total_replacements

def main():
    print("Fetching proxy repositories from Nexus...")
    print(f"REPO_ROOT is set to: {CONFIG['repo_root']}")

    repos_json_str = safe_request(CONFIG['nexus_api_url'])
    if not repos_json_str:
        print("Warning: Failed to fetch repositories from Nexus. Continuing without proxy replacements.", file=sys.stderr)
        sys.exit(0)

    try:
        repos_data = json.loads(repos_json_str)
    except Exception:
        print("Warning: Failed to parse Nexus response. Continuing without proxy replacements.", file=sys.stderr)
        sys.exit(0)

    proxy_mappings = {}
    for repo in repos_data:
        if repo.get('type') == 'proxy' and 'attributes' in repo:
            proxy_attrs = repo.get('attributes', {}).get('proxy', {})
            remote_url = proxy_attrs.get('remoteUrl')
            nexus_url = repo.get('url')
            if remote_url and nexus_url:
                proxy_mappings[remote_url] = nexus_url

    if not proxy_mappings:
        print("Warning: No proxy repositories found. Continuing without proxy replacements.", file=sys.stderr)
        sys.exit(0)

    print("Found proxy repositories:")
    for remote_url, nexus_url in proxy_mappings.items():
        print(f"  {remote_url} -> {nexus_url}")

    maven_files, maven_replacements = process_maven_replacements(
        CONFIG['repo_root'],
        CONFIG['maven_file_patterns'],
        CONFIG['exclude_dirs'],
        proxy_mappings
    )

    print("\n=========================================")
    print("Processing GitHub URLs in dependencies.cmake files...")
    print("=========================================")

    github_proxy_url = None
    for repo in repos_data:
        if repo.get('name') == CONFIG['github_repo_name']:
            github_proxy_url = repo.get('url')
            break

    if github_proxy_url:
        github_proxy_prefix = f"{github_proxy_url.rstrip('/')}/"
        print(f"Using GitHub proxy: {github_proxy_prefix}")
    else:
        print("Warning: GitHub proxy repository not found in Nexus. Skipping GitHub URL replacements.")
        github_proxy_prefix = None

    cmake_files, cmake_replacements = process_github_replacements(
        CONFIG['repo_root'],
        CONFIG['cmake_file_pattern'],
        CONFIG['exclude_dirs'],
        github_proxy_prefix
    )

    print("\n=========================================")
    print("Summary:")
    print(f"  Maven files processed: {maven_files}")
    print(f"  Maven replacements: {maven_replacements}")
    print(f"  dependencies.cmake files processed: {cmake_files}")
    print(f"  GitHub URL replacements: {cmake_replacements}")
    print("=========================================")
    print("Done!")
    sys.exit(0)

if __name__ == '__main__':
    main()

