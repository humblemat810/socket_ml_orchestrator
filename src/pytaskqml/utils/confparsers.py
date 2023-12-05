import configparser
def worker_config_parser(config: configparser.ConfigParser, args):
    

    management_port = args.management_port
    if management_port is None:
        management_port = config.get("worker", "management-port")
    management_port = int(management_port)

    port = args.port 
    if port is None:
        port = config.get("worker", "port")    
    port = int(port)
    log_level = args.log_level
    if log_level is None:
        log_level = config.get("logger", "level")
    log_screen = args.log_screen
    if log_screen is None:
        log_screen = config.get("logger", "logscreen")
        
    if type(log_screen) is str:
        if log_screen.upper() == 'FALSE':
            log_screen = False
        elif log_screen.upper() == 'TRUE':
            log_screen = True
        else:
            raise ValueError(f"incorrect config for log_screen, expected UNION[FALSE, TRUE], get {log_screen}")
    return {'port': port, 'log_level': log_level, "log_screen" : log_screen, 'management_port': management_port}

def dispatcher_side_worker_config_parser(config: configparser.ConfigParser, parsed_args):
    worker_config = []
    i = 1
    while f'worker.{i}' in config:
        worker_section = config[f'worker.{i}']
        location = worker_section['location']
        location = location.split(':')
        if len(location) == 1:
            location = location[0]
            if location != 'local':
                raise (ValueError('when no port specified, only local is supported, remote format is [hostname:port]'))
            
        else:
            location, port = location
            location = (str(location),int(port))
        worker_config.append({"location": location,
                            "min_start_processing_length": int(worker_section.get("min_start_processing_length"))
                            })
        i+=1
        if parsed_args.n_worker is not None:
            if i <= parsed_args.n_worker:
                continue
            else:
                break
    if parsed_args.n_worker is not None:
        while i <= parsed_args.n_worker:
            port = str(int(port) + 1)
            worker_config.append({"location": (str(location),int(port)),
                            "min_start_processing_length": int(worker_section.get("min_start_processing_length"))
                            })
            i+=1
    return worker_config