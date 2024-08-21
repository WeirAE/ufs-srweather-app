"""
The run script for chgres_cube
"""

import datetime as dt
import os
import sys
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path

from uwtools.api.file import link as uwlink
from uwtools.api.chgres_cube import Chgres_Cube
from uwtools.api.config import get_yaml_config


parser = ArgumentParser(
    description="Script that runs chgres_cube via uwtools API",
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
    help="The cycle in ISO8601 format (e.g. 2024-07-15T18)",
    required=True,
    type=dt.datetime.fromisoformat,
)
parser.add_argument(
    "--key-path",
    help="Dot-separated path of keys leading through the config to the driver's YAML block",
    metavar="KEY[.KEY...]",
    required=True,
)
parser.add_argument(
    "--member",
    default="000",
    help="The 3-digit ensemble member number.",
)
args = parser.parse_args()

os.environ["member"] = args.member

# Extract driver config from experiment config
chgres_cube_driver = Chgres_Cube(
    config=args.config_file,
    cycle=args.cycle,
    key_path=[args.key_path],
)
rundir = Path(chgres_cube_driver.config["rundir"])
print(f"Will run in {rundir}")
# Run chgres_cube
chgres_cube_driver.run()

if not (rundir / "runscript.chgres_cube.done").is_file():
    print("Error occurred running chgres_cube. Please see component error logs.")
    sys.exit(1)

# Deliver output data
expt_config = get_yaml_config(args.config_file)
chgres_cube_config = expt_config[args.key_path]

links = {}
for label in chgres_cube_config["output_file_labels"]:
    # deepcopy here because desired_output_name is parameterized within the loop
    expt_config_cp = get_yaml_config(deepcopy(expt_config.data))
    expt_config_cp.dereference(
        context={
            "cycle": args.cycle,
            "leadtime": args.leadtime,
            "file_label": label,
            **expt_config_cp,
        }
    )
    chgres_cube_block = expt_config_cp[args.key_path]
    desired_output_fn = chgres_cube_block["desired_output_name"]
    upp_output_fn = rundir / f"{label.upper()}.GrbF{int(args.leadtime.total_seconds() // 3600):02d}"
    links[desired_output_fn] = str(upp_output_fn)

uwlink(target_dir=rundir.parent, config=links)