import gerrychain
from gerrychain import Graph, Partition, MarkovChain
from gerrychain.updaters import Tally, cut_edges
from gerrychain.proposals import recom
from gerrychain.constraints import within_percent_of_ideal_population
from gerrychain.accept import always_accept
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from shapely import wkt
import os
import pandas as pd
import boto3          


# Use non-interactive backend for matplotlib
plt.switch_backend('Agg')

# Figure out which run I am
ARRAY_IDX = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX", "0"))
RUN_NUM = ARRAY_IDX + 1

print(f"[INFO] Starting run #{RUN_NUM}")

# Create results directory
os.makedirs("results", exist_ok=True)


############################
##### SET UP ###############
############################

# Load the data
df = pd.read_csv("partition_0.csv")

# Convert the 'geometry' column from WKT to Shapely geometries
df['geometry'] = df['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df, geometry='geometry')

# set the coordinate reference system (CRS)
known_crs = "EPSG:3857" 
gdf.crs = known_crs
gdf = gdf.set_index('GEOID')

# Drop duplicate indices
gdf = gdf[~gdf.index.duplicated(keep='first')]

# Build connected graph from GeoDataFrame
graph = Graph.from_geodataframe(gdf)

gdf['SCHOOL_ID'] = gdf['SCHOOL_ID'].astype('str')
graph.add_data(gdf, columns=['pop', 'SCHOOL_ID'])

############################
##### MCMC SETUP ###########
############################
print("Starting MCMC process...")

# Updaters
updaters = {
    "population": Tally("pop", alias="population"), # Use 'pop' column
    "cut_edges": cut_edges, # Counts how many boundaries are cut
}

# Check our inidtial partition
initial_partition = Partition(
    graph,
    assignment="SCHOOL_ID", # Use 'SCHOOL_ID' column
    updaters=updaters
)

print("Initial population per school:")
print(initial_partition["population"])

num_districts = len(initial_partition.parts)
ideal_population = sum(initial_partition["population"].values()) / num_districts
pop_tolerance = 0.40

print(f"Ideal population: {ideal_population:.0f}, Tolerance: +/- {pop_tolerance*100}%")

# Create the initial partition so it satisfies the population constraints
print("\n--- Generating a new Starting Partition ---")
from gerrychain.tree import recursive_tree_part
num_districts = len(initial_partition.parts) 
new_assignment = recursive_tree_part(
    graph,                               
    parts=range(num_districts),           
    pop_target=ideal_population,          
    pop_col="pop",                        
    epsilon=pop_tolerance               
)

# Define the NEW partition
new_initial_partition = Partition(
    graph,
    assignment=new_assignment,
    updaters=updaters
)
print("New initial partition created.")

# Define constraints USING THE NEW partition
constraints = [
    within_percent_of_ideal_population(new_initial_partition, pop_tolerance)
]
print("Constraints defined.")

# Define the proposal function USING THE NEW partition
def proposal_function(partition):
    return recom(
        partition,
        pop_col="pop",
        pop_target=ideal_population,
        epsilon=pop_tolerance,
        node_repeats=50
    )

print("Proposal function defined.")


# Create the MarkovChain
print("\n--- Creating MarkovChain ---")

try:
    chain = MarkovChain(
        proposal=proposal_function,     
        constraints=constraints,
        accept=always_accept, 
        initial_state=new_initial_partition, 
        total_steps=1000 
    )
    print("MarkovChain created successfully.")

except ValueError as e:
    print(f"ERROR: The new plan might still fail constraints: {e}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

############################
##### SIMULATION ###########
############################
def run_mcmc(chain_num):
    chain_num = chain_num
    print(f"\n--- Running the Chain{chain_num} ---")
    results = []
    last_partition_for_loop = new_initial_partition

    # Check if we should run the chain
    if len(new_initial_partition.parts) >= 2:
        for i, partition_step in enumerate(chain): # Use a different name here
            if (i + 1) % 100 == 0:
                print(f"  ... Step {i + 1}")
            
            district_populations = list(partition_step["population"].values())
            results.append({
                'step': i + 1,
                'cut_edges': len(partition_step['cut_edges']),
                'min_pop': min(district_populations),
                'max_pop': max(district_populations),
                'pop_range': max(district_populations) - min(district_populations),
            })
            last_partition_for_loop = partition_step # Update with the latest
    else:
        print("Not enough districts to run the chain.")

    print("Chain finished (or skipped).")

    ############################
    ##### RESULTS ##############
    ############################
    # Add the final district assignment back to our GeoDataFrame
    new_gdf = gdf.copy()
    new_gdf['FINAL_DISTRICT'] = new_gdf.index.map(last_partition_for_loop.assignment)
    new_gdf['FINAL_DISTRICT'] = new_gdf['FINAL_DISTRICT'].astype(str)

    assignment = new_gdf['FINAL_DISTRICT']

    # Calculate income distribution
    new_gdf["inc_times_pop"] = new_gdf["indinc"] * new_gdf["pop"]
    new_gdf.reset_index('GEOID', inplace=True)
    districts = (
        new_gdf.dissolve(
            by="FINAL_DISTRICT",
            aggfunc={"inc_times_pop": "sum", "pop": "sum"},     # <-- just sums
            as_index=False,  # keeps the ID as a column, not index
        )
        .assign(w_mean_iinc=lambda d: d["inc_times_pop"] / d["pop"])
        .drop(columns=["inc_times_pop"])                        # optional clean-up
    )

    districts.plot(column="w_mean_iinc", cmap="OrRd", legend=True)
    plt.title("Weighted Income Distribution")
    plt.savefig(f"results/partition_rank{chain_num}.png", dpi=300)
    plt.close()

    return assignment



# -------- Run the script and save results to s3 --------
assignment = run_mcmc(RUN_NUM)  

# Save locally inside the container
local_csv = f"/tmp/run_{RUN_NUM}.csv"
assignment.to_csv(local_csv, header=["district"], index_label="GEOID")

#  (optional) copy to S3 so you can merge later
S3_BUCKET = os.getenv("RESULT_BUCKET")          # set in the job definition
if S3_BUCKET:
    s3 = boto3.client("s3")
    s3.upload_file(local_csv, S3_BUCKET, f"runs/run_{RUN_NUM}.csv")
    print(f"[INFO] Uploaded to s3://{S3_BUCKET}/runs/run_{RUN_NUM}.csv")

print("[INFO] Done.")
