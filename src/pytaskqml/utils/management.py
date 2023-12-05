
def shutdown_url_request(url):
    import requests
    cnt_retry = 0
    cnt_retry_max = 3
    errs= []
    while True:
        try:
            requests.get(url)
        except ConnectionRefusedError: # server port shutdown
            break
        except requests.exceptions.ConnectionError as e:
            import urllib3
            if isinstance(e.args[0],  urllib3.exceptions.ProtocolError):
                uePE = e.args[0]
                if uePE.args[0] == 'Connection aborted.':
                    if isinstance(uePE.args[1], ConnectionResetError):
                        CRE = uePE.args[1]
                        if CRE.args[0] == 10054 and CRE.args[1] == 'An existing connection was forcibly closed by the remote host':
                            break # ok for this error
            elif isinstance(e.args[0],  urllib3.exceptions.MaxRetryError):
                ueMRE = e.args[0]
                if isinstance(ueMRE.reason, urllib3.exceptions.NewConnectionError):
                    break
            else:
                raise
        except Exception as e: # network disrupt, server busy
            cnt_retry += 1
            errs.append(e)
            if cnt_retry > cnt_retry_max:
                print(errs)
                raise
            continue
    return True