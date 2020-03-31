import os
import sys

from cottoncandy import options

args = sys.argv
paths_to_search_list = args[1:]

replacement_dict = {
    options.config.get('login', 'access_key'): "FAKE_ACCESS_KEY",
    options.config.get('login', 'secret_key'): "FAKE_SECRET_KEY",
    options.config.get('login', 'endpoint_url'): "FAKE_ENDPOINT_URL",
    options.config.get('basic', 'default_bucket'): "FAKE_DEFAULT_BUCKET",
    }

max_depth = 4
intermediate_path = ""
for depth in range(max_depth):
    intermediate_path = os.path.join(intermediate_path, "**")
    for path_to_search in paths_to_search_list:
        "Searching files in {path_to_search}".format(path_to_search=path_to_search)
        for suffix in ["html", "js", "txt"]:
            for word, replacement in replacement_dict.items():
                cmd = "rpl -iR {word} {replacement} {path_to_search}".format(word=word, replacement=replacement, path_to_search=os.path.join(path_to_search, intermediate_path, "*.{suffix}".format(suffix=suffix)))
                print(cmd)
                print(os.system(cmd))
