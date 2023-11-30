import pstats

# Load the profiling data from a .prof file
profiler = pstats.Stats('dispatcher.prof')

# Sort the profiling data by total time
profiler.sort_stats('tottime')

# Print the sorted profiling data
profiler.print_stats(30)