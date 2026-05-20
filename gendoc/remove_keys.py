import os
import sys

from cottoncandy import options

args = sys.argv
paths_to_search_list = args[1:]

if len(paths_to_search_list) == 0:
    print("No paths provided; nothing to sanitize.")
    sys.exit(1)

replacement_dict = {
    options.config.get('login', 'access_key'): "FAKE_ACCESS_KEY",
    options.config.get('login', 'secret_key'): "FAKE_SECRET_KEY",
    options.config.get('login', 'endpoint_url'): "FAKE_ENDPOINT_URL",
    options.config.get('basic', 'default_bucket'): "FAKE_DEFAULT_BUCKET",
    }

suffixes = {".html", ".js", ".txt"}

files_sanitized = 0
files_changed = 0
errors = []

def should_replace(word):
    if isinstance(word, bool) or (word is None) or (not isinstance(word, str)):
        return False
    return len(word) != 0

def sanitize_file(file_path, replacements):
    global files_sanitized, files_changed
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
            content = handle.read()
    except OSError:
        errors.append(file_path)
        return

    original = content
    for word, replacement in replacements.items():
        if not should_replace(word):
            continue
        content = content.replace(word, replacement)

    if content == original:
        files_sanitized += 1
        return

    try:
        with open(file_path, "w", encoding="utf-8", errors="ignore") as handle:
            handle.write(content)
        files_sanitized += 1
        files_changed += 1
    except OSError:
        errors.append(file_path)

for path_to_search in paths_to_search_list:
    if not os.path.exists(path_to_search):
        errors.append(path_to_search)
        continue

    for root, _, files in os.walk(path_to_search):
        for filename in files:
            if os.path.splitext(filename)[1] not in suffixes:
                continue
            sanitize_file(os.path.join(root, filename), replacement_dict)

print("Sanitized {count} files; updated {changed} files; {errors} errors.".format(
    count=files_sanitized,
    changed=files_changed,
    errors=len(errors),
))

if len(errors) > 0:
    print("Sanitization failed; see file access errors in logs.")
    sys.exit(1)
