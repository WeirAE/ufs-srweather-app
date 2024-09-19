#!/usr/bin/env python
"""
The run script for UPP
"""

import datetime as dt
import logging
import os
import re
import sys
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path

from uwtools.api.fs import link as uwlink
from uwtools.api.logging import use_uwtools_logger
from uwtools.api.upp import UPP
from uwtools.api.config import get_yaml_config


def _timedelta_from_str(tds):
    """
    Return a timedelta parsed from a leadtime string.

    :param tds: The timedelta string to parse.
    """
    if matches := re.match(r"(\d+)(:(\d+))?(:(\d+))?", tds):
        h, m, s = [int(matches.groups()[n] or 0) for n in (0, 2, 4)]
        return dt.timedelta(hours=h, minutes=m, seconds=s)
    msg = "Specify leadtime as hours[:minutes[:seconds]]"
    logging.error(msg)
    sys.exit(1)

def _walk_key_path(config, key_path):
    """
    Navigate to the sub-config at the end of the path of given keys.
    """
    keys = []
    pathstr = "<unknown>"
    for key in key_path:
        keys.append(key)
        pathstr = " -> ".join(keys)
        try:
            subconfig = config[key]
        except KeyError:
            logging.error(f"Bad config path: {pathstr}")
            raise
        if not isinstance(subconfig, dict):
            logging.error(f"Value at {pathstr} must be a dictionary")
            sys.exit(1)
        config = subconfig
    return config

def parse_args(argv):
    """
    Parse arguments for the script.
    """
    parser = ArgumentParser(
        description="Script that runs UPP via uwtools API.",
    )
    parser.add_argument(
        "-c",
        "--config-file",
        metavar="PATH",
        required=True,
        help="Path to experiment config file.",
        type=Path,
    )
    parser.add_argument(
        "--cycle",
        help="The cycle in ISO8601 format (e.g. 2024-07-15T18).",
        required=True,
        type=dt.datetime.fromisoformat,
    )
    parser.add_argument(
        "--leadtime",
        help="The leadtime as hours[:minutes[:seconds]].",
        required=True,
        type=_timedelta_from_str,
    )
    parser.add_argument(
        "--key-path",
        help="Dot-separated path of keys leading through the config to the driver's YAML block.",
        metavar="KEY[.KEY...]",
        required=True,
        type=lambda s: s.split("."),
    )
    parser.add_argument(
        "--member",
        default="000",
        help="The 3-digit ensemble member number.",
    )
    return parser.parse_args(argv)


def run_upp(config_file, cycle, leadtime, key_path, member):
    """
    Setup and run the UPP Driver.
    """

    # The experiment config will have {{ MEMBER | env }} expressions in it that need to be
    # dereferenced during driver initialization.
    os.environ["MEMBER"] = member

    # Run the UPP program via UW driver
    upp_driver = UPP(
        config=config_file,
        cycle=cycle,
        leadtime=leadtime,
        key_path=key_path,
    )
    rundir = Path(upp_driver.config["rundir"])
    logging.info(f"Will run UPP in {rundir}")
    upp_driver.run()

    if not (rundir / "runscript.upp.done").is_file():
        logging.error("Error occurred running UPP. Please see component error logs.")
        sys.exit(1)

    # Deliver output data to a common location above the rundir.
    expt_config = get_yaml_config(config_file)
    upp_config = _walk_key_path(expt_config, key_path)

    links = {}
    for label in upp_config["output_file_labels"]:
        # deepcopy here because desired_output_name is parameterized within the loop
        expt_config_cp = get_yaml_config(deepcopy(expt_config.data))
        expt_config_cp.dereference(
            context={
                "cycle": cycle,
                "leadtime": leadtime,
                "file_label": label,
                **expt_config_cp,
            }
        )
        upp_block = _walk_key_path(expt_config_cp, key_path)
        desired_output_fn = upp_block["desired_output_name"]
        upp_output_fn = (
            rundir
            / f"{label.upper()}.GrbF{int(leadtime.total_seconds() // 3600):02d}"
        )
        links[desired_output_fn] = str(upp_output_fn)

    uwlink(target_dir=rundir.parent, config=links)


if __name__ == "__main__":

    use_uwtools_logger()

    args = parse_args(sys.argv[1:])
    run_upp(
        config_file=args.config_file,
        cycle=args.cycle,
        leadtime=args.leadtime,
        key_path=args.key_path,
        member=args.member,
    )
