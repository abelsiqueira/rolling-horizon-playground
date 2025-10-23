# Used to create the rolling horizon test case
using CSV
using DataFrames
using DuckDB
using MetaGraphsNext
using Plots
using TulipaClustering
using TulipaEnergyModel # pkg> add TulipaEnergyModel#main
using TulipaIO
using TulipaBuilder # pkg> add https://github.com/TulipaEnergy/TulipaBuilder.jl

tulipa = TulipaData{String}()

add_asset!(tulipa, "solar", :producer; capacity = 150.0, initial_units = 1.0)
add_asset!(tulipa, "thermal", :producer; capacity = 150.0, initial_units = 1.0)
add_asset!(
    tulipa,
    "battery",
    :storage;
    capacity = 30.0,
    capacity_storage_energy = 100.0,
    initial_units = 1.0,
    initial_storage_units = 1.0,
    initial_storage_level = 0.0,
    storage_charging_efficiency = 0.9,
    storage_discharging_efficiency = 0.9,
)
add_asset!(tulipa, "demand", :consumer; peak_demand = 100.0, consumer_balance_sense = "==")

add_asset!(
    tulipa,
    "demand-bid-001",
    :consumer;
    peak_demand = 100.0,
    consumer_balance_sense = "==",
    unit_commitment = true,
    unit_commitment_integer = true,
    unit_commitment_method = "basic",
)
add_asset!(
    tulipa,
    "demand-bid-002",
    :consumer;
    peak_demand = 100.0,
    consumer_balance_sense = "==",
    unit_commitment = true,
    unit_commitment_integer = true,
    unit_commitment_method = "basic",
)

### flow
add_flow!(tulipa, "solar", "demand")
add_flow!(tulipa, "thermal", "demand"; operational_cost = 50.0)
add_flow!(tulipa, "battery", "demand")
add_flow!(tulipa, "demand", "battery")

add_flow!(tulipa, "demand", "demand-bid-001"; operational_cost = -10.0)
add_flow!(tulipa, "demand", "demand-bid-002"; operational_cost = -20.0) # This is preferred because of the higher price

### profiles
df = DataFrame(CSV.File("rolling_horizon.csv"))
df.timestep = df.day * 24 + df.hour
sort!(df, :timestep)
horizon_length = size(df, 1)

attach_profile!(tulipa, "solar", :availability, 2030, df[!, "solar_pu"])
attach_profile!(tulipa, "demand", :demand, 2030, df[!, "demand_MW"] / 100)

zero_profile = zeros(horizon_length)
bids_profiles = Dict("001" => copy(zero_profile), "002" => copy(zero_profile))
bids_profiles["001"][22:26] .= 1.0
bids_profiles["002"][22:26] .= 1.0
attach_profile!(tulipa, "demand-bid-001", :demand, 2030, bids_profiles["001"])
attach_profile!(tulipa, "demand-bid-002", :demand, 2030, bids_profiles["002"])

connection = create_connection(tulipa)

### clustering
dummy_cluster!(connection)
TulipaBuilder.create_case_study_csv_folder(connection, "Bids")

### populate_with_defaults
populate_with_defaults!(connection)

## Solving and visualising

profiles = DataFrame(DuckDB.query(connection, "FROM profiles"))
demand = sort(profiles[profiles.profile_name.=="demand-demand-2030", :], :timestep)
solar = sort(profiles[profiles.profile_name.=="solar-availability-2030", :], :timestep)

horizon_length = size(demand, 1)
move_forward = 24
opt_window_length = 48

energy_problem = TulipaEnergyModel.run_scenario(
    connection;
    # move_forward,
    # opt_window_length;
    show_log = false,
    model_file_name = "bids.lp",
    # save_rolling_solution = true, # optional: saves intermediate solutions
)

big_table_rh = DataFrame(DuckDB.query(
    connection,
    """
    WITH cte_outgoing AS (
        SELECT
            var.from_asset AS asset,
            var.time_block_start AS timestep,
            SUM(var.solution) AS solution,
        FROM var_flow AS var
        WHERE rep_period = 1 AND year = 2030
        GROUP BY asset, timestep
    ), cte_incoming AS (
        SELECT
            var.to_asset AS asset,
            var.time_block_start AS timestep,
            SUM(var.solution) AS solution,
        FROM var_flow AS var
        WHERE rep_period = 1 AND year = 2030
        GROUP BY asset, timestep
    ), cte_unified AS (
        SELECT
            cte_outgoing.asset,
            cte_outgoing.timestep,
            coalesce(cte_outgoing.solution, 0.0) AS outgoing,
            coalesce(cte_incoming.solution, 0.0) AS incoming,
        FROM cte_outgoing
        LEFT JOIN cte_incoming
            ON cte_outgoing.asset = cte_incoming.asset
            AND cte_outgoing.timestep = cte_incoming.timestep
    ) FROM cte_unified
""",
))

timestep = range(extrema(big_table_rh.timestep)...)
thermal = sort(big_table_rh[big_table_rh.asset.=="thermal", :], :timestep).outgoing
solar = sort(big_table_rh[big_table_rh.asset.=="solar", :], :timestep).outgoing
discharge = sort(big_table_rh[big_table_rh.asset.=="battery", :], :timestep).outgoing
charge = sort(big_table_rh[big_table_rh.asset.=="battery", :], :timestep).incoming

horizon_length = length(timestep)
y = hcat(thermal, solar, discharge)
Plots.plot(;
    ylabel = "MW",
    xlims = (1, horizon_length),
    xticks = 1:12:horizon_length,
    size = (800, 150),
    legend = :outerright,
)

Plots.areaplot!(timestep, y; label = ["thermal" "solar" "discharge"])
Plots.areaplot!(timestep, -charge; label = "charge")