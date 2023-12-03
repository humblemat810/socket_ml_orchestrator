import pstats
import yappi
import pickle
with open('dispatcher.ystat', 'rb') as f:
    pickle.load(f)
stats = yappi.get_func_stats()
stats._add_from_YSTAT('dispatcher.ystat')
yappi.YFuncStats(files = ['dispatcher.ystat'])
# Load the profiling data from a .prof file
profiler = pstats.Stats('dispatcher.prof')

# Sort the profiling data by total time
profiler.sort_stats('tottime')

# Print the sorted profiling data
profiler.print_stats(30)