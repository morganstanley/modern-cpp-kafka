load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "gtest",
    urls = ["https://github.com/google/googletest/archive/refs/tags/release-1.11.0.zip"],
    strip_prefix = "googletest-release-1.11.0",
)

http_archive(
    name = "rapidjson",
    build_file = "//customrules:rapidjson.BUILD",
    urls = ["https://github.com/Tencent/rapidjson/archive/refs/heads/master.zip"],
    strip_prefix="rapidjson-master",
)

