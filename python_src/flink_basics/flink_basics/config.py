jar_dir = "/usr/local/workdir/python_src/flink_basics/flink_basics/jars"

jar_files = [
    "flink-connector-kafka-1.17.1.jar",
    "kafka-clients-3.2.3.jar"
]


def get_jar_file_paths():
    jar_file_paths = ["file://"+jar_dir + "/" + jar_file for jar_file in jar_files]
    return jar_file_paths
