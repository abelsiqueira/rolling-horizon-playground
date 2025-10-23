# Used to create the rolling horizon test case
using CSV
using DataFrames
using DuckDB
using MetaGraphsNext
using TulipaClustering
using TulipaEnergyModel
using TulipaIO
using TulipaBuilder

function _validate_one_rep_period(connection)
    for row in DuckDB.query(
        connection,
        "SELECT year, max(rep_period) as num_rep_periods
        FROM rep_periods_data
        GROUP BY year
        ",
    )
        if row.num_rep_periods > 1
            error("We should have only 1 rep period for rolling horizon")
        end
    end
end

# We are using TulipaBuilder as main development tool because we need RP = 1

tulipa = TulipaData{String}()

add_asset!(tulipa, "solar", :producer; capacity = 150.0, initial_units = 1.0)
add_asset!(tulipa, "thermal", :producer; capacity = 150.0, initial_units = 1.0)
add_asset!(
    tulipa,
    "battery",
    :storage;
    capacity = 10.0,
    capacity_storage_energy = 40.0,
    initial_units = 1.0,
    initial_storage_units = 1.0,
    initial_storage_level = 0.0,
    storage_charging_efficiency = 0.9,
    storage_discharging_efficiency = 0.9,
)
add_asset!(tulipa, "demand", :consumer; peak_demand = 100.0, consumer_balance_sense = "==")

### flow
add_flow!(tulipa, "solar", "demand")
add_flow!(tulipa, "solar", "battery")
add_flow!(tulipa, "thermal", "demand"; operational_cost = 50.0)
add_flow!(tulipa, "thermal", "battery")
add_flow!(tulipa, "battery", "demand")

### profiles
df = DataFrame(CSV.File("rolling_horizon.csv"))
df.timestep = df.day * 24 + df.hour
sort!(df, :timestep)
horizon_length = size(df, 1)

attach_profile!(tulipa, "solar", :availability, 2030, df[!, "solar_pu"])
attach_profile!(tulipa, "demand", :demand, 2030, df[!, "demand_MW"] / 100)

connection = create_connection(tulipa)

### clustering
dummy_cluster!(connection)

### populate_with_defaults
populate_with_defaults!(connection)

TulipaBuilder.create_case_study_csv_folder(connection, "JuMPTest")
