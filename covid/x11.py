import subprocess
import os
import plotly
import getpass
def start(x11_id=200):
    global xvfb_proc
    if(getpass.getuser()!='covidstat-bot'): x11_id+=1000
    display = ":{:d}".format(x11_id)
    for line in os.popen("ps ax | grep Xvfb | grep -v grep"):
        fields = line.split()
        if len(fields)>= 7:
            pid = fields[0]
            dps = fields[6]
            if dps == display:
                print("Kill dead Xvfb, pid: ", pid, " DISPLAY: ", dps)
                os.system("sudo kill "+pid)
    x11_display = display
    xvfb_proc = subprocess.Popen(["sudo", "/usr/bin/Xvfb", x11_display], preexec_fn=os.setpgrp, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("started Xvfb, pid: ", xvfb_proc.pid, " DISPLAY: ", display)
    os.environ['DISPLAY'] = x11_display
    plotly.io.orca.ensure_server()

def stop():
    global xvfb_proc
    plotly.io.orca.shutdown_server()
    kill_proc = subprocess.check_output(["sudo","kill","{}".format(os.getpgid(xvfb_proc.pid))])