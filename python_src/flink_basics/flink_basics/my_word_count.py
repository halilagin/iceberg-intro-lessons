from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.common import ExecutionConfig

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Create a simple data source
    data = env.from_collection(
        collection=["hello world", "hello flink", "flink is great"],
        type_info=Types.STRING()
    )

    # Apply transformations: split, group, and sum
    word_counts = (
        data.flat_map(
            lambda line: [(word, 1) for word in line.split()],
            output_type=Types.TUPLE([Types.STRING(), Types.INT()])
        )
        .key_by(lambda x: x[0])
        .sum(1)
    )

    # Print the result to stdout
    word_counts.print()

    # Execute the program
    env.execute("PyFlink Word Count Example")

if __name__ == "__main__":
    main()

