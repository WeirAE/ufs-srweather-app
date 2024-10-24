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


# pylint: disable=too-many-locals, too-many-statements, too-many-branches
def run_chgres_cube(config_file, cycle, key_path, member):
    """
    Setup and run the chgres_cube Driver.
    """

    # dereference expressions during driver initialization
    expt_config = get_yaml_config(config_file)
    CRES = expt_config["workflow"]["CRES"]
    os.environ["CRES"] = CRES
    os.environ["MEMBER"] = member

    # set universal variables
    cyc = str(expt_config["workflow"]["DATE_FIRST_CYCL"])[8:10]
    dot_ensmem = (
        f".mem{member}"
        if (
            expt_config["user"]["RUN_ENVIR"] == "nco"
            and expt_config["global"]["DO_ENSEMBLE"]
            and member
        )
        else ""
    )
    nco_net = expt_config["nco"]["NET_default"]

    # Extract driver config from experiment config
    chgres_cube_driver = ChgresCube(
        config=config_file,
        cycle=cycle,
        key_path=key_path,
    )
    rundir = Path(chgres_cube_driver.config["rundir"])
    print(f"Will run in {rundir}")

    # Dereference cycle for file paths
    expt_config_cp = get_yaml_config(deepcopy(expt_config.data))
    expt_config_cp.dereference(
        context={
            "cycle": cycle,
            **expt_config_cp,
        }
    )
    chgres_cube_config = _walk_key_path(expt_config_cp, key_path)
    input_type = chgres_cube_config["chgres_cube"]["namelist"]["update_values"][
        "config"
    ].get("input_type")

    # update config for ics task, run and stage data
    if "task_make_ics" in key_path:
        varsfilepath = chgres_cube_config["input_files_metadata_path"]
        shconfig = _parse_var_defns(varsfilepath)
        extrn_config_fns = shconfig["EXTRN_MDL_FNS"]
        extrn_config_fhrs = shconfig["EXTRN_MDL_FHRS"]

        if input_type == "grib2":
            fn_grib2 = extrn_config_fns[0]
            update = {"grib2_file_input_grid": fn_grib2}
        else:
            fn_atm = extrn_config_fns[0]
            fn_sfc = extrn_config_fns[1]
            update = {"atm_files_input_grid": fn_atm, "sfc_files_input_grid": fn_sfc}
        if expt_config["task_get_extrn_ics"]["EXTRN_MDL_NAME_ICS"] in [
            "HRRR",
            "RAP",
        ]:
            if expt_config["workflow"]["SDF_USES_RUC_LSM"] is True:
                update["nsoill_out"] = 9
        else:
            if expt_config["workflow"]["SDF_USES_THOMPSON_MP"] is True:
                update["thomp_mp_climo_file"] = expt_config["workflow"][
                    "THOMPSON_MP_CLIMO_FP"

                ]

        update_cfg = {
            "task_make_ics": {
                "chgres_cube": {"namelist": {"update_values": {"config": update}}}
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

        # Deliver output data to a common location above the rundir.
        links = {}
        tile_rgnl = expt_config["constants"]["TILE_RGNL"]
        nh0 = expt_config["constants"]["NH0"]

        output_dir = os.path.join(rundir.parent, "INPUT")
        os.makedirs(output_dir, exist_ok=True)
        links[
            f"{nco_net}.t{cyc}z{dot_ensmem}.gfs_data.tile{tile_rgnl}.halo{nh0}.nc"
        ] = str(rundir / f"out.atm.tile{tile_rgnl}.nc")
        links[
            f"{nco_net}.t{cyc}z{dot_ensmem}.sfc_data.tile{tile_rgnl}.halo{nh0}.nc"
        ] = str(rundir / f"out.sfc.tile{tile_rgnl}.nc")
        links[f"{nco_net}.t{cyc}z.gfs_ctrl.nc"] = str(rundir / f"gfs_ctrl.nc")
        links[f"{nco_net}.t{cyc}z{dot_ensmem}.gfs_bndy.tile{tile_rgnl}.f000.nc"] = str(
            rundir / f"gfs.bndy.nc"
        )
        uwlink(target_dir=output_dir, config=links)

    #  update config for lbcs task, loop run and stage data
    else:
        fn_sfc = ""
        varsfilepath = chgres_cube_config["input_files_metadata_path"]
        shconfig = _parse_var_defns(varsfilepath)
        extrn_config_fns = shconfig["EXTRN_MDL_FNS"]
        extrn_config_fhrs = shconfig["EXTRN_MDL_FHRS"]
        num_fhrs = len(extrn_config_fhrs)

        bcgrp10 = 0
        bcgrpnum10 = 1
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
                if expt_config["task_get_extrn_lbcs"]["EXTRN_MDL_NAME_LBCS"] not in [
                    "HRRR",
                    "RAP",
                ]:
                    if expt_config["workflow"]["SDF_USES_THOMPSON_MP"] is True:
                        update["thomp_mp_climo_file"] = expt_config["workflow"][
                            "THOMPSON_MP_CLIMO_FP"
                        ]

                update_cfg = {
                    "task_make_lbcs": {
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

                # Deliver output data to a common location above the rundir.
                links = {}

                lbc_spec_fhrs = extrn_config_fhrs[i]
                lbc_offset_fhrs = expt_config_cp["task_get_extrn_lbcs"][
                    "EXTRN_MDL_LBCS_OFFSET_HRS"
                ]
                fcst_hhh = int(lbc_spec_fhrs) - int(lbc_offset_fhrs)
                fcst_hhh_FV3LAM = f"{fcst_hhh:03d}"

                lbc_input_fn = rundir / f"gfs.bndy.nc"
                output_dir = os.path.join(rundir.parent, "INPUT")
                os.makedirs(output_dir, exist_ok=True)
                lbc_output_fn = str(
                    f"{nco_net}.t{cyc}z{dot_ensmem}"
                    f".gfs_bndy.tile7.f{fcst_hhh_FV3LAM}.nc"
                )
                links[lbc_output_fn] = str(lbc_input_fn)
                uwlink(target_dir=output_dir, config=links)

    # error message
    if not (rundir / "runscript.chgres_cube.done").is_file():
        print("Error occurred running chgres_cube. Please see component error logs.")
        sys.exit(1)


if __name__ == "__main__":

    use_uwtools_logger()

    args = parse_args(sys.argv[1:])
    run_chgres_cube(
        config_file=args.config_file,
        cycle=args.cycle,
        key_path=args.key_path,
        member=args.member,
    )
