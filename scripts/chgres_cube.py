"""
The run script for chgres_cube
"""

import datetime as dt
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

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


varsfilepath = chgres_cube_driver.config["task_make_ics"]["input_files_metadata_path"]
extrn_config_fns = get_sh_config(varsfilepath)[EXTRN_MDL_FNS]

# make_ics
fn_atm = extrn_config_fns[0]
fn_sfc = extrn_config_fns[1]

# Loop the run of chgres_cube for the forecast length
num_fhrs = chgres_cube_driver.config["workflow"]["FCST_LEN_HRS"]
bcgrp10 = 0
bcgrpnum10 = 1
for ii in range(bcgrp10, num_fhrs, bcgrpnum10):
    i = ii + bcgrpnum10
    if i < num_fhrs:
        print(f"group ${bcgrp10} processes member ${i}")
        fn_atm = f"${{EXTRN_MDL_FNS[${i}]}}"
        fn_sfc= "$EXTRN_MDL_FNS[1]"
    
        if ics_or_lbcs == "LBCS":
            chgres_cube_driver.config["task_make_lbcs"]["chgres_cube"]["namelist"]["update_values"]["config"]["atm_files_input_grid"] = fn_atm
        else ics_or_lbcs == "ICS":
            chgres_cube_driver.config["task_make_ics"]["chgres_cube"]["namelist"]["update_values"]["config"]["atm_files_input_grid"] = fn_atm
            chgres_cube_driver.config["task_make_ics"]["chgres_cube"]["namelist"]["update_values"]["config"]["sfc_files_input_grid"] = fn_sfc
        chgres_cube_driver.run()

if not (rundir / "runscript.chgres_cube.done").is_file():
    print("Error occurred running chgres_cube. Please see component error logs.")
    sys.exit(1)

# Deliver output data
expt_config = get_yaml_config(args.config_file)
chgres_cube_config = expt_config[args.key_path]
