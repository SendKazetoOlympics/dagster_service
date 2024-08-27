from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import do_stuff
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[do_stuff],
)
