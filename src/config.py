import argparse



def get_args():
    argparser = argparse.ArgumentParser(description="arguments")

    # Running SciStream on Chameleon
    argparser.add_argument('--p2cs_ep', help="p2cs endpoint name", required=True)
    argparser.add_argument('--p2cs_ip', help="IP address of the s2cs on producer side", required=True)
    argparser.add_argument('--p2cs_listener', help="listerner's IP of p2cs", required=True)   
    argparser.add_argument('--prod_ip', help="producer's IP address", required=True)     
    
    argparser.add_argument('--c2cs_ep', help="c2cs endpoint name", required=True)
    argparser.add_argument('--c2cs_ip', help="IP address of the s2cs on consumer side", required=True)
    argparser.add_argument('--c2cs_listener', help="listerner's IP of c2cs", required=True)
    argparser.add_argument('--cons_ip', help="consumer's IP address", required=True)        

    # General Parameters
    argparser.add_argument('--inbound_ep', help="inbound initiator endpoint name", required=True)                 # the server certification should be specified
    argparser.add_argument('--outbound_ep', help="outbound initiator endpoint name", required=True)               # the server certification should be specified
    argparser.add_argument('--inbound_ip', help='inbound IP address', required=True)
    argparser.add_argument('--outbound_ip', help='outbound IP address', required=True)

    argparser.add_argument('-c', '--cleanup', action="store_true", help="clean up the orphan processes", default=True)  #for test purposes, otherwise should be default=False
    argparser.add_argument('-v', '--verbose', action="store_true", help="Initiate a new stream connection", default=False)

    # SciStream Parameters
    argparser.add_argument('--sync_port', help="syncronization port",default="5000")
    argparser.add_argument('--type', help= "proxy type: Nginx, HaproxySubprocess, StunnelSubprocess, Haproxy", default="StunnelSubprocess")
    argparser.add_argument('--rate', type=int, help="transfer rate",default=10000)         #TODO: Add it to the command lines
    argparser.add_argument('--num_conn', type=int, help="THe number of specified ports", default=11)
    argparser.add_argument('--inbound_src_ports', type=str, help="Comma-separated list of inbound receiver ports", default="5074,5075,5076,5077,5078,5079,5080,5081,5082,5083,5084")            #dynamically is increased by the s2uc and then is read from the log file
    argparser.add_argument('--outbound_dst_ports', type=str, help="Comma-separated list of outbound receiver ports", default="5050,5100,5101,5102,5103,5104,5105,5106,5107,5108,5109,5110")     #dynamically is increased by the s2uc and then is read from the log file
    

    return argparser.parse_args()