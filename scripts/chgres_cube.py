#!/usr/bin/env python
"""
The run script for chgres_cube
"""

import datetime as dt
import logging
import os
import re
import sys
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path

from uwtools.api.chgres_cube import ChgresCube
from uwtools.api.config import get_yaml_config
from uwtools.api.fs import link as uwlink
from uwtools.api.logging import use_uwtools_logger


def _parse_var_defns(file):
    var_dict = {}
    with open(file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()

                if value.startswith("(") and value.endswith(")"):
                    items = re.findall(r"\((.*?)\)", value)
                    if items:
                        value = [item.strip() for item in items[0].split()]
                        var_dict[key] = value
    return var_dict


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
        type=lambda s: s.split("."),
    )
    parser.add_argument(
        "--member",
        default="000",
        help="The 3-digit ensemble member number.",
    )
    return parser.parse_args(argv)


# pylint: disable=too-many-locals, too-many-statements
def run_chgres_cube(config_file, cycle, key_path, member):
    """
    Setup and run the chgres_cube Driver.
    """

    # The experiment config will have {{ MEMBER | env }} expressions in it that need to be
    # dereferenced during driver initialization.

    os.environ["member"] = member

    expt_config = get_yaml_config(config_file)

    # dereference expressions during driver initialization
    CRES = expt_config["workflow"]["CRES"]
    os.environ["CRES"] = CRES

    # Extract driver config from experiment config
    chgres_cube_driver = ChgresCube(
        config=config_file,
        cycle=cycle,
        key_path=key_path,
    )

    # Dereference cycle for file paths
    expt_config_cp = get_yaml_config(deepcopy(expt_config.data))
    expt_config_cp.dereference(
        context={
            "cycle": cycle,
            **expt_config_cp,
        }
    )

    chgres_cube_config = _walk_key_path(expt_config_cp, key_path)
    # update fn_atm and fn_sfc for ics task
    if "task_make_ics" in key_path:
        rundir = Path(chgres_cube_driver.config["rundir"])
        print(f"Will run in {rundir}")
        varsfilepath = chgres_cube_config["input_files_metadata_path"]
        shconfig = _parse_var_defns(varsfilepath)
        extrn_config_fns = shconfig["EXTRN_MDL_FNS"]
        extrn_config_fhrs = shconfig["EXTRN_MDL_FHRS"]

        input_type = chgres_cube_config["chgres_cube"]["namelist"]["update_values"][
            "config"
        ].get("input_type")
        if input_type == "grib2":
            fn_grib2 = extrn_config_fns[0]
            update = {"grib2_file_input_grid": fn_grib2}
        else:
            fn_atm = extrn_config_fns[0]
            fn_sfc = extrn_config_fns[1]
            update = {"atm_files_input_grid": fn_atm, "sfc_files_input_grid": fn_sfc}

        update_cfg = {
            "task_make_ics": {
                "chgres_cube": {"namelist": {"update_values": {"config": update}}}
            }
        }
        expt_config_cp.update_from(update_cfg)
        logging.info(f"updated config: {expt_config_cp}")

        # reinstantiate driver
        chgres_cube_driver = ChgresCube(
            config=expt_config_cp,
            cycle=cycle,
            key_path=key_path,
        )
        chgres_cube_driver.run()

    # Loop the run of chgres_cube for the forecast length if lbcs
    else:
        rundir = Path(chgres_cube_driver.config["rundir"])
        print(f"Will run in {rundir}")
        fn_sfc = ""
        varsfilepath = chgres_cube_config["input_files_metadata_path"]
        shconfig = _parse_var_defns(varsfilepath)
        extrn_config_fns = shconfig["EXTRN_MDL_FNS"]
        extrn_config_fhrs = shconfig["EXTRN_MDL_FHRS"]
        num_fhrs = len(extrn_config_fhrs)

        input_type = chgres_cube_config["chgres_cube"]["namelist"]["update_values"][
            "config"
        ].get("input_type")
        bcgrp10 = 0
        bcgrpnum10 = 1
        update = {}
        for ii in range(bcgrp10, num_fhrs, bcgrpnum10):
            i = ii + bcgrp10
            if i < num_fhrs:
                print(f"group {bcgrp10} processes member {i}")
                if input_type == "grib2":
                    fn_grib2 = extrn_config_fns[i]
                    update = {"grib2_file_input_grid": fn_grib2}
                else:
                    fn_atm = extrn_config_fns[i]
                    update = {"atm_files_input_grid": fn_atm}

                update_cfg = {
                    "task_make_ics": {
                        "chgres_cube": {
                            "namelist": {"update_values": {"config": update}}
                        }
                    }
                }
                expt_config_cp.update_from(update_cfg)

                # reinstantiate driver
                chgres_cube_driver = ChgresCube(
                    config=expt_config_cp,
                    cycle=cycle,
                    key_path=key_path,
                )
                chgres_cube_driver.run()

    # error message
    if not (rundir / "runscript.chgres_cube.done").is_file():
        print("Error occurred running chgres_cube. Please see component error logs.")
        sys.exit(1)

    # Deliver output data
    expt_config = get_yaml_config(config_file)
    chgres_cube_config = _walk_key_path(expt_config, key_path)

    # Move initial condition, surface, control, and 0-th hour lateral bound-
    # ary files to ICs_BCs directory.
    links = {}
    for label in chgres_cube_config["output_file_labels"]:
        # deepcopy here because desired_output_name is parameterized within the loop
        expt_config_cp = get_yaml_config(deepcopy(expt_config.data))
        expt_config_cp.dereference(
            context={
                "cycle": cycle,
                "file_label": label,
                **expt_config_cp,
            }
        )
        lbc_block = _walk_key_path(expt_config_cp, key_path)
        lbc_input_fn = "gfs.bndy.nc"
        lbc_spec_fhrs = extrn_config_fhrs[i]
        lbc_offset_fhrs = lbc_block["EXTRN_MDL_LBCS_OFFSET_HRS"]
        nco_net = expt_config["nco"]["NET_default"]
        dot_ensmem = f".mem{member}"
        fcst_hhh = lbc_spec_fhrs - lbc_offset_fhrs
        fcst_hhh_FV3LAM = f"{fcst_hhh:03d}"

        lbc_output_fn = (
            rundir
            / f"{nco_net}.{cycle}{dot_ensmem}.gfs_bndy.tile7.f{fcst_hhh_FV3LAM}.nc"
        )

        links[lbc_input_fn] = str(lbc_output_fn)

    uwlink(target_dir=rundir.parent, config=links)


if __name__ == "__main__":

    use_uwtools_logger()

    args = parse_args(sys.argv[1:])
    run_chgres_cube(
        config_file=args.config_file,
        cycle=args.cycle,
        key_path=args.key_path,
        member=args.member,
    )
