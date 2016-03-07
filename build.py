import subprocess
import os


if '__main__' == __name__:
    os.environ['GOPATH'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_gopath')
    subprocess.check_call('go install github.com/esdb/kafka-agent', shell=True)