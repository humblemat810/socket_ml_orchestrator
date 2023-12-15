
def main():
    import importlib
    import yappi
    import threading
    threading.main_thread().name = str(sys.argv)
    yappi.set_clock_type('WALL')
    # Specify the module and function names as strings
    module_name, function_name = args.entrypoint.split('.')
    sys.argv[0] = module_name + ".py"

    # Import the specific function from the module dynamically
    imported_module = importlib.import_module(module_name)
    function = getattr(imported_module, function_name)
    yappi.start()
    try:
        function()
    except Exception as e:
        print(e)
    yappi.stop()
    statsf = yappi.get_func_stats()
    statsth = yappi.get_thread_stats()
    import pickle
    with open("./" + args.outfile_prefix + '.ystatpickle', 'wb') as f:
        pickle.dump({"thread_stat": statsth, "func_stat": statsf}, f)
    
    """'test call:'

    python yappi_main_wrapper.py demo_case1_dispatcher.py "--management-port", "18000", "--config", 'dispatcher_case2.ini'
    """
if __name__ == '__main__':
    import argparse
    wrapper_parser = argparse.ArgumentParser()
    wrapper_parser.add_argument('entrypoint',type = str,  help='pythonfile.main')
    wrapper_parser.add_argument('outfile_prefix',type = str,  help='prefix for outfile')
    args, remaining_args = wrapper_parser.parse_known_args()
    import sys
    print(sys.argv)
    sys.argv.pop(1)
    sys.argv.pop(1)
    import threading
    th = threading.Thread(target = main, name = args.outfile_prefix)
    th.start()
    th.join()