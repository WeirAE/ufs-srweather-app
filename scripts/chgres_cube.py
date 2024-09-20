"""
The run script for chgres_cube
"""

import datetime as dt
import os
import sys
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path

from uwtools.api.chgres_cube import ChgresCube
from uwtools.api.config import get_sh_config, get_yaml_config
from uwtools.api.fs import link as uwlink
from uwtools.api.logging import use_uwtools_logger


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
use_uwtools_logger()
args = parser.parse_args()

os.environ["member"] = args.member

expt_config = get_yaml_config(args.config_file)
chgres_cube_config = expt_config[args.key_path]

# dereference expressions during driver initialization
CRES = expt_config["workflow"]["CRES"]
os.environ["CRES"] = CRES

# Extract driver config from experiment config
chgres_cube_driver = ChgresCube(
    config=args.config_file,
    cycle=args.cycle,
    key_path=[args.key_path],
)

# update fn_atm and fn_sfc for ics task
if args.key_path == "task_make_ics":
    rundir = Path(chgres_cube_driver.config["rundir"])
    print(f"Will run in {rundir}")
    varsfilepath = expt_config["task_make_ics"][
        "input_files_metadata_path"
    ]
    extrn_config_fns = get_sh_config(varsfilepath)["EXTRN_MDL_FNS"]
    extrn_config_fhrs = get_sh_config(varsfilepath)["EXTRN_MDL_FHRS"]

    fn_atm = extrn_config_fns[0]
    fn_sfc = extrn_config_fns[1]

    chgres_cube_driver.run()

# Loop the run of chgres_cube for the forecast length if lbcs
else:
    rundir = Path(chgres_cube_driver.config["rundir"])
    print(f"Will run in {rundir}")
    fn_sfc = ""
    num_fhrs = expt_config["workflow"]["FCST_LEN_HRS"]
    bcgrp10 = 0
    bcgrpnum10 = 1
    for ii in range(bcgrp10, num_fhrs, bcgrpnum10):
        i = ii + bcgrpnum10
        if i < num_fhrs:
            print(f"group ${bcgrp10} processes member ${i}")
            fn_atm = f"${{EXTRN_MDL_FNS[${i}]}}"

            chgres_cube_driver.config["namelist"]["update_values"][
                "config"
            ]["atm_files_input_grid"] = fn_atm
            # reinstantiate driver
            chgres_cube_driver = ChgresCube(
                config=expt_config,
                cycle=args.cycle,
                key_path=[args.key_path],
            )
            chgres_cube_driver.run()


# error message
if not (rundir / "runscript.chgres_cube.done").is_file():
    print("Error occurred running chgres_cube. Please see component error logs.")
    sys.exit(1)

# Deliver output data
expt_config = get_yaml_config(args.config_file)
chgres_cube_config = expt_config[args.key_path]


# Move initial condition, surface, control, and 0-th hour lateral bound-
# ary files to ICs_BCs directory.
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
    lbc_block = expt_config_cp[args.key_path]
    lbc_input_fn = "gfs.bndy.nc"
    lbc_spec_fhrs = extrn_config_fhrs[i]
    lbc_offset_fhrs = expt_config["task_get_extrn_lbcs"][
        "EXTRN_MDL_LBCS_OFFSET_HRS"
    ]
    nco_net = expt_config["nco"]["NET_default"]
    dot_ensmem = f".mem{ args.member }"
    fcst_hhh = lbc_spec_fhrs - lbc_offset_fhrs
    fcst_hhh_FV3LAM = print(f"fcst_hhh:03d")

    lbc_output_fn = (
        rundir
        / f"{nco_net}.{args.cycle}{dot_ensmem}.gfs_bndy.tile7.f{fcst_hhh_FV3LAM}.nc"
    )

    links[lbc_input_fn] = str(lbc_output_fn)

uwlink(target_dir=rundir.parent, config=links)
