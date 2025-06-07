from mpi4py import MPI
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

# --------------- SET UP ---------------
# Use non-interactive backend for matplotlib
plt.switch_backend('Agg')

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

RUN_NUM = rank + 1

print(f"[INFO] Process {rank+1}/{size} starting run #{RUN_NUM}")

# Create results directory
os.makedirs("results", exist_ok=True)
os.makedirs("output", exist_ok=True)

# Load the data (each process loads independently)
df = pd.read_csv("partition_0.csv")

# Convert geometry
df['geometry'] = df['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df, geometry='geometry')

# Set CRS
known_crs = "EPSG:3857" 
gdf.crs = known_crs
gdf = gdf.set_index('GEOID')

# Drop duplicates
gdf = gdf[~gdf.index.duplicated(keep='first')]

# --------------- MCMC CONFIG USING GERRYCHAIN --------------
# Build graph
graph = Graph.from_geodataframe(gdf)
gdf['SCHOOL_ID'] = gdf['SCHOOL_ID'].astype('str')
graph.add_data(gdf, columns=['pop', 'SCHOOL_ID'])

print(f"Process {rank}: Starting MCMC process...")

# Updaters
updaters = {
    "population": Tally("pop", alias="population"),
    "cut_edges": cut_edges,
}

# Initial partition
initial_partition = Partition(
    graph,
    assignment="SCHOOL_ID",
    updaters=updaters
)

if rank == 0:  # Only print when we are at the root process
    print("Initial population per school:")
    print(initial_partition["population"])

num_districts = len(initial_partition.parts)
ideal_population = sum(initial_partition["population"].values()) / num_districts
pop_tolerance = 0.40

if rank == 0: # same here
    print(f"Ideal population: {ideal_population:.0f}, Tolerance: +/- {pop_tolerance*100}%")

# Generate new starting partition (with different seed for each process)
print(f"Process {rank}: Generating a new Starting Partition...")
from gerrychain.tree import recursive_tree_part
import random

# Set different random seed for each process
random.seed(42 + rank)

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
print(f"Process {rank}: New initial partition created.")

# Define constraints
constraints = [
    within_percent_of_ideal_population(new_initial_partition, pop_tolerance)
]

# Define proposal function
def proposal_function(partition):
    return recom(
        partition,
        pop_col="pop",
        pop_target=ideal_population,
        epsilon=pop_tolerance,
        node_repeats=50
    )

# Create MarkovChain
print(f"Process {rank}: Creating MarkovChain...")
chain = MarkovChain(
    proposal=proposal_function,     
    constraints=constraints,
    accept=always_accept, 
    initial_state=new_initial_partition, 
    total_steps=1000 
)

# ------------- DEFINE MCMC FUNCTION --------------
def run_mcmc(rank, chain_num):
    print(f"Process {rank}: Running Chain {chain_num}...")
    results = []
    last_partition_for_loop = new_initial_partition

    if len(new_initial_partition.parts) >= 2:
        for i, partition_step in enumerate(chain):
            if (i + 1) % 100 == 0:
                print(f"Process {rank}: Step {i + 1}")
            
            district_populations = list(partition_step["population"].values())
            results.append({
                'step': i + 1,
                'cut_edges': len(partition_step['cut_edges']),
                'min_pop': min(district_populations),
                'max_pop': max(district_populations),
                'pop_range': max(district_populations) - min(district_populations),
            })
            last_partition_for_loop = partition_step
    else:
        print(f"Process {rank}: Not enough districts to run the chain.")

    print(f"Process {rank}: Chain finished.")

    # Add final district assignment
    new_gdf = gdf.copy()
    new_gdf['FINAL_DISTRICT'] = new_gdf.index.map(last_partition_for_loop.assignment)
    new_gdf['FINAL_DISTRICT'] = new_gdf['FINAL_DISTRICT'].astype(str)

    assignment = new_gdf['FINAL_DISTRICT']

    # Calculate weighted income distribution
    new_gdf["inc_times_pop"] = new_gdf["indinc"] * new_gdf["pop"]
    new_gdf.reset_index('GEOID', inplace=True)
    districts = (
        new_gdf.dissolve(
            by="FINAL_DISTRICT",
            aggfunc={"inc_times_pop": "sum", "pop": "sum"},
            as_index=False,
        )
        .assign(w_mean_iinc=lambda d: d["inc_times_pop"] / d["pop"])
        .drop(columns=["inc_times_pop"])
    )

    # Save plot
    districts.plot(column="w_mean_iinc", cmap="OrRd", legend=True)
    plt.title(f"Weighted Income Distribution - Run {chain_num}")
    plt.savefig(f"results/partition_rank{chain_num}.png", dpi=300)
    plt.close()

    return assignment, results

# -------------- RUN MCMC --------------
# Run the script (we want to save assignment schemes)
assignment, results = run_mcmc(rank, RUN_NUM)

# Save results locally (as a test)
output_csv = f"output/run_{RUN_NUM}.csv"
assignment.to_csv(output_csv, header=["district"], index_label="GEOID")

# Save statistics
stats_df = pd.DataFrame(results)
stats_csv = f"output/stats_{RUN_NUM}.csv"
stats_df.to_csv(stats_csv, index=False)

print(f"Process {rank}: Results saved to {output_csv}")
print(f"Process {rank}: Statistics saved to {stats_csv}")
print(f"Process {rank}: Done.")

# Wait for all processes to finish
comm.Barrier()

if rank == 0:
    print("All processes completed!")
    print("Results are in the output/ directory")
    print("Plots are in the results/ directory")