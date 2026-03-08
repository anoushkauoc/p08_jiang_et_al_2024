"""
Load project configurations from .env files or from the command line.

Provides easy access to paths and credentials used in the project.
Meant to be used as an imported module.

If `settings.py` is run on its own, it will create the appropriate
directories.

Configuration precedence:
    1. Command line arguments
    2. Environment variables / .env file
    3. settings.py defaults
    4. Inline default passed to config()
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from platform import system

from decouple import config as _config


def find_all_caps_cli_vars(argv=sys.argv):
    """Find all ALL_CAPS command line arguments passed like:
    --DATA_DIR=/path/to/data
    or
    --DATA_DIR /path/to/data
    """
    result = {}
    i = 0
    while i < len(argv):
        arg = argv[i]

        # Handle --VAR=value
        if arg.startswith("--") and "=" in arg and arg[2:].split("=")[0].isupper():
            var_name, value = arg[2:].split("=", 1)
            result[var_name] = value

        # Handle --VAR value
        elif arg.startswith("--") and arg[2:].isupper() and i + 1 < len(argv):
            var_name = arg[2:]
            value = argv[i + 1]
            if not value.startswith("--"):
                result[var_name] = value
                i += 1

        i += 1
    return result


cli_vars = find_all_caps_cli_vars()


# ---------------------------------------------------------------------
# Base helpers
# ---------------------------------------------------------------------
def get_os() -> str:
    os_name = system()
    if os_name == "Windows":
        return "windows"
    if os_name in {"Darwin", "Linux"}:
        return "nix"
    return "unknown"


def get_stata_exe(os_type: str) -> str:
    """Get the name of the Stata executable based on the OS type."""
    if os_type == "windows":
        return "StataMP-64.exe"
    if os_type == "nix":
        return "stata-mp"
    raise ValueError("Unknown OS type")


def if_relative_make_abs(path: Path, base_dir: Path) -> Path:
    """If path is relative, make it absolute relative to project root."""
    path = Path(path)
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


# ---------------------------------------------------------------------
# Core defaults
# ---------------------------------------------------------------------
if "BASE_DIR" in cli_vars:
    BASE_DIR_DEFAULT = Path(cli_vars["BASE_DIR"]).resolve()
else:
    BASE_DIR_DEFAULT = Path(__file__).absolute().parent.parent

if "OS_TYPE" in cli_vars:
    OS_TYPE_DEFAULT = cli_vars["OS_TYPE"]
else:
    OS_TYPE_DEFAULT = get_os()

if "STATA_EXE" in cli_vars:
    STATA_EXE_DEFAULT = cli_vars["STATA_EXE"]
else:
    STATA_EXE_DEFAULT = get_stata_exe(OS_TYPE_DEFAULT)


defaults = {
    # Project structure
    "BASE_DIR": BASE_DIR_DEFAULT,
    "OS_TYPE": OS_TYPE_DEFAULT,
    "STATA_EXE": STATA_EXE_DEFAULT,
    "DATA_DIR": if_relative_make_abs(Path("_data"), BASE_DIR_DEFAULT),
    "MANUAL_DATA_DIR": if_relative_make_abs(Path("data_manual"), BASE_DIR_DEFAULT),
    "OUTPUT_DIR": if_relative_make_abs(Path("_output"), BASE_DIR_DEFAULT),
    "SRC_DIR": if_relative_make_abs(Path("src"), BASE_DIR_DEFAULT),

    # Dates
    "START_DATE": datetime.strptime("1913-01-01", "%Y-%m-%d"),
    "END_DATE": datetime.strptime("2025-12-31", "%Y-%m-%d"),

    # FFIEC project-specific settings
    "REPORT_DATE": "03312022",
    "REPORT_DATE_SLASH": "03/31/2022",
}


def config(
    var_name,
    default=None,
    cast=None,
    settings_py_defaults=defaults,
    cli_vars=cli_vars,
    convert_dir_vars_to_abs_path=True,
):
    """
    Read configuration variable with precedence:
        1. Command line arguments
        2. Environment variables / .env
        3. settings.py defaults
        4. Inline default passed to config()
    """

    # 1. Command line arguments
    if var_name in cli_vars and cli_vars[var_name] is not None:
        value = cli_vars[var_name]
        if cast is not None:
            value = cast(value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            value = if_relative_make_abs(Path(value), settings_py_defaults["BASE_DIR"])
        return value

    # 2. Environment variables / .env
    env_sentinel = object()
    env_value = _config(var_name, default=env_sentinel)
    if env_value is not env_sentinel:
        if cast is not None:
            env_value = cast(env_value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            env_value = if_relative_make_abs(
                Path(env_value), settings_py_defaults["BASE_DIR"]
            )
        return env_value

    # 3. settings.py defaults
    if var_name in settings_py_defaults:
        value = settings_py_defaults[var_name]
        if cast is not None and not isinstance(value, Path):
            value = cast(value)
        return value

    # 4. Inline default
    if default is not None:
        value = default
        if cast is not None:
            value = cast(value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            value = if_relative_make_abs(Path(value), settings_py_defaults["BASE_DIR"])
        return value

    raise ValueError(
        f"Configuration variable '{var_name}' is not defined.\n"
        f"Set it via:\n"
        f"  1. Command line: --{var_name}=value\n"
        f"  2. Environment variable: export {var_name}=value\n"
        f"  3. .env file: {var_name}=value"
    )


def create_directories():
    config("DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("MANUAL_DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("OUTPUT_DIR").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    create_directories()
    print(f"BASE_DIR: {config('BASE_DIR')}")
    print(f"DATA_DIR: {config('DATA_DIR')}")
    print(f"OUTPUT_DIR: {config('OUTPUT_DIR')}")
    print(f"REPORT_DATE: {config('REPORT_DATE')}")
    print(f"REPORT_DATE_SLASH: {config('REPORT_DATE_SLASH')}")
