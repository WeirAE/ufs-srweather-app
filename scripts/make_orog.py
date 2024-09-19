#!/usr/bin/env python
"""
The run script for making the orography files for the experiment.
"""

import glob
import logging
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

from uwtools.api.driver import Driver
from uwtools.api.filter_topo import FilterTopo
from uwtools.api.logging import use_uwtools_logger
from uwtools.api.orog import Orog
from uwtools.api.orog_gsl import OrogGSL
from uwtools.api.shave import Shave
from uwtools.api.config import get_yaml_config


def link_files(dest_dir, files):
    """
    Link a given list of files to the destination directory using the same file names.
    """
    for fpath in files:
        path = Path(fpath)
        linkname = dest_dir / path.name
        if linkname.is_symlink():
            linkname.unlink()
        logging.info(f"Linking {linkname} -> {path}")
        linkname.symlink_to(path)


def parse_args(argv):
    """
    Parse arguments for the script.
    """
    parser = ArgumentParser(
        description="Script that runs the make_orog task via uwtools API.",
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
        "--key-path",
        help="Dot-separated path of keys leading through the config to the tasks's YAML block.",
        metavar="KEY[.KEY...]",
        required=True,
        type=lambda s: s.split("."),
    )

    return parser.parse_args(argv)


def make_orog(config_file, key_path):
    """
    Run the series of drivers needed to produce FV3 fix files related to topography.
    """
    expt_config = get_yaml_config(config_file)
    make_orog_dict = expt_config
    for kp in key_path:
        make_orog_dict = make_orog_dict[kp]
    make_orog_config = get_yaml_config(make_orog_dict)
    task_rundir = Path(make_orog_config["rundir"])
    logging.info(f"Will run make_orog in {task_rundir}")

    # The experiment config will have {{ CRES | env }} expressions in it that need to be
    # dereferenced during driver initialization.
    cres = expt_config["workflow"]["CRES"]
    os.environ["CRES"] = cres

    # Destination of important files from this process
    fix_lam_path = Path(expt_config["workflow"]["FIXlam"])

    # Run the orog program
    run_driver(driver_class=Orog, config_file=config_file, key_path=key_path)

    # Run orog_gsl only when using GSL's orography drag suite
    ccpp_phys_suite = expt_config["workflow"]["CCPP_PHYS_SUITE"]
    orog_drag_suites = [
        "FV3_RAP",
        "FV3_HRRR",
        "FV3_GFS_v15_thompson_mynn_lam3km",
        "FV3_GFS_v17_p8",
    ]
    if ccpp_phys_suite in orog_drag_suites:
        orog_gsl_driver = run_driver(
            driver_class=OrogGSL, config_file=config_file, key_path=key_path
        )
        output_files = [
            orog_gsl_driver.rundir / f"{cres}_oro_data_{i}.tile7.halo0.nc"
            for i in ("ss", "ls")
        ]
        link_files(dest_dir=fix_lam_path, files=output_files)

    # Run filter_topo
    run_driver(driver_class=FilterTopo, config_file=config_file, key_path=key_path)

    # Run shave for 0- and 4-cell-wide halo
    for sub_path in ["shave0", "shave4"]:
        driver = run_driver(
            driver_class=Shave,
            config_file=config_file,
            key_path=[*key_path, sub_path],
        )

    # Link shave output to fix directory (shave config points to task dir, not shave rundir)
    link_files(
        dest_dir=fix_lam_path,
        files=glob.glob(str(task_rundir / f"{cres}*.nc")),
    )

    # Mark the successful completion of the script on disk.
    Path(task_rundir / "make_orog_task_complete.txt").touch()


def run_driver(driver_class, config_file, key_path):
    """
    Initialize and run the provided UW driver.

    Return the configured object.
    """
    driver = driver_class(
        config=config_file,
        key_path=key_path,
    )
    rundir = Path(driver.config["rundir"])
    logging.info(f"Will run {driver.driver_name()} in {rundir}")
    driver.run()

    if not (rundir / f"runscript.{driver.driver_name()}.done").is_file():
        logging.error(
            f"Error occurred running {driver.driver_name()}. Please see component error logs."
        )
        sys.exit(1)
    return driver


if __name__ == "__main__":

    use_uwtools_logger()

    args = parse_args(sys.argv[1:])
    make_orog(
        config_file=args.config_file,
        key_path=args.key_path,
    )
