from dagster import load_assets_from_package_name

CORE = "core"

core_assets = load_assets_from_package_name(
    "lean_data_eng.assets.core", group_name=CORE
)


ALL_ASSETS = [*core_assets]
