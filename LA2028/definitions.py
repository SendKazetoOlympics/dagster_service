from dagster import Definitions, load_assets_from_package_module

from .assets import core

core_assets = load_assets_from_package_module(core, group_name="core")

all_assets = [*core_assets]

defs = Definitions(
    assets=all_assets,
)
