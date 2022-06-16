import inspect
import os
import traceback
from argparse import ArgumentParser, Action
from datetime import datetime

arg_store = None


def save(inputs):
    def closure():
        return inputs

    return closure


def get_args(keys):
    return arg_store()[keys]


def save_args_configs(args):
    global arg_store
    args['runId'] = 'testing'
    arg_store = save(args)
    return True


def arg_parser():
    parser = ArgumentParser(description='Spark Driver Controller')

    class StoreDictKeyPair(Action):
        def __call__(self, par, namespace, values, option_string=None):
            my_dict = {}
            for kv in values.split(";"):
                k, v = kv.split("=")
                my_dict[k] = v
            setattr(namespace, self.dest, my_dict)

    parser.add_argument('-m', '--module', type=str, help='Full method name', default=None, required=True)
    parser.add_argument('-e', '--environment', help='Environment name', default=None, required=True)
    parser.add_argument('-pr', '--params', type=str, action=StoreDictKeyPair, metavar='KEY1=VAL1;KEY2=VAL2;...',
                        help='Key value pairs', default=None, required=False)
    parser.add_argument('-r', '--runId', type=str, help='Airflow run_id', required=False)
    parser.add_argument('-c', '--config_path', type=str, help='Project yaml name, default=project', required=False)
    parser.add_argument('-p', '--packages', type=str, help='Spark packages', required=False)
    parser.add_argument('-pj', '--project_file', type=str, default=None, help='Project yaml name, default=None',
                        required=False)
    parser.add_argument('-ss', '--sparkSettings', type=str, nargs='+', help='Spark settings', required=False)
    parser.add_argument('-d', '--dryrun', help='Always read from prod but write to environment specified in -e',
                        action='store_true', required=False)
    return parser


if __name__ == '__main__':
    sTime = datetime.now()
    parser = arg_parser()
    args, unknown = parser.parse_known_args()
    print(args)
    print(vars(args))

    try:
        config_path = '{}/configs'.format(os.path.dirname(os.path.abspath(inspect.stack()[0][1])))
        print(config_path)
        config_path = args.config_path if args.config_path else config_path

        if not save_args_configs(vars(args)):
            raise SystemError('Config Failed')

        module = get_args('module').rpartition('.')
        kwargs = get_args('params') if get_args('params') else {}
        print(module[0])
        print(module[1])
        print(module[2])
        exec('from {} import {}'.format(module[0], module[2]))
        exec('{}(**kwargs)'.format(module[2]))

    except Exception as e:
        raise Exception("Application stopped due to: \n" + traceback.format_exc())

    eTime = datetime.now()
    fTime = eTime - sTime
