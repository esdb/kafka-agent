#!/usr/bin/env python2.7
import subprocess
import os
import argparse
import distutils.spawn


if '__main__' == __name__:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.path.realpath('/opt/esdb/kafka-agent') != os.path.realpath(BASE_DIR):
        raise Exception('must link project to "/opt/esdb/kafka-agent" to build')
    GOPATH = os.path.join(BASE_DIR, '_gopath')
    os.environ['GOPATH'] = GOPATH
    if not os.path.exists(os.path.join(GOPATH, 'src', 'github.com', 'esdb', 'kafka-agent')):
        try:
            os.makedirs(os.path.join(GOPATH, 'src', 'github.com', 'esdb'))
        except:
            pass
        os.symlink(os.path.join(BASE_DIR, 'kafka-agent'),
                   os.path.join(GOPATH, 'src', 'github.com', 'esdb', 'kafka-agent'))
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', default='build')
    args = parser.parse_args()
    if 'build' == args.command:
        subprocess.check_call('go get github.com/esdb/kafka-agent', shell=True)
    elif 'dev-env' == args.command:
        os.execv(distutils.spawn.find_executable('supervisord'), ['supervisord', '-c', 'build/etc/supervisord.conf'])
    else:
        raise Exception('unknown command: %s' % args.command)