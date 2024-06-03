import argparse
from pathlib import Path
from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from collections import defaultdict
from os import environ, getcwd


_DATABASE = environ.get("SNOWFLAKE_DATABASE")

LOG = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
LOG.addHandler(handler)
LOG.setLevel(logging.DEBUG)


def process_schema(environment, schema, files=False):
    change_table = f"{_DATABASE}.{schema}.CHANGE_HISTORY"
    root_folder = Path(f'data_model/{environment}/{schema}')
    cmd = ['python',
            'cicd/schemachange.py',
            '-f', root_folder,
            '-c', change_table,
            '--config-folder', './cicd']
    if files:
        cmd.append('--change-file-list')
        cmd.extend(files)

    proc = Popen(cmd,stderr=PIPE, stdout=PIPE)
    out, err = proc.communicate()
    if proc.returncode == 0:
        return out, err
    raise RuntimeError(
        f"Build Failed. See output.\n Output:{out}\n Error:{err}\n")


def process_output(procout, schema):
    LOG.info("----------------------------"
             f"{schema} Output"
             "----------------------------")
    for line in procout[0].decode('UTF-8').split('\n'):
        if ('Ignoring' in line or 'Found' in line) \
                and LOG.level != logging.DEBUG:
            continue
        LOG.info(line)
    if procout[1]:
        for line in procout[1].decode('UTF-8').split('\n'):
            LOG.info(line)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('environment', help="Environment for deployment")
    parser.add_argument('-f','--files', nargs='*', help='List of changed files.')
    parser.add_argument('-s', '--schemas', nargs='*', help="List of schemas")
    args = parser.parse_args()

    if args.files:
        args.schemas = defaultdict(list)
        for file in args.files:
            args.schemas[file.split('/')[2]].append(Path(getcwd(),file))

    exceptions = {}
    with ThreadPoolExecutor(max_workers=9) as pool:
        # Bit of a hack, but the below creates a dictionary with the
        # keys being subprocesses and the values being the schema that
        # is being processed.
        if args.files:
            futures = {pool.submit(process_schema, args.environment, schema, args.schemas[schema]):
                    schema for schema in args.schemas}
        else:
            futures = {pool.submit(process_schema, args.environment, schema):
                       schema for schema in args.schemas}
        for future in as_completed(futures):
            schema = futures[future]
            try:
                out = future.result()
                process_output(out, schema)
            except Exception as e:
                logging.warning(e)
                exceptions[schema] = e
    if len(exceptions) > 0:
        raise RuntimeError("Build Failed see output.")


if __name__ == '__main__':
    main()
