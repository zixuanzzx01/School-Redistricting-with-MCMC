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
import random

# --------------- SET UP ---------------
plt.switch_backend('Agg')

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Configuration for 100 total runs
TOTAL_RUNS = 100
RUNS_PER_PROCESS = TOTAL_RUNS // size 
EXTRA_RUNS = TOTAL_RUNS % size         # 100 % 8 = 4 extra runs

# Some processes get one extra run
if rank < EXTRA_RUNS:
    my_runs = RUNS_PER_PROCESS + 1
    start_run = rank * (RUNS_PER_PROCESS + 1) + 1
else:
    my_runs = RUNS_PER_PROCESS
    start_run = EXTRA_RUNS * (RUNS_PER_PROCESS + 1) + (rank - EXTRA_RUNS) * RUNS_PER_PROCESS + 1

end_run = start_run + my_runs - 1

print(f"[INFO] Process {rank+1}/{size} will run {my_runs} chains (runs {start_run}-{end_run})")

# Create results directory
os.makedirs("results", exist_ok=True)
os.makedirs("output", exist_ok=True)

# Load data once (shared by all runs on this process)
df = pd.read_csv("partition_0.csv")
df['geometry'] = df['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df, geometry='geometry')
known_crs = "EPSG:3857" 
gdf.crs = known_crs
gdf = gdf.set_index('GEOID')
gdf = gdf[~gdf.index.duplicated(keep='first')]

# Build graph once
graph = Graph.from_geodataframe(gdf)
gdf['SCHOOL_ID'] = gdf['SCHOOL_ID'].astype('str')
graph.add_data(gdf, columns=['pop', 'SCHOOL_ID'])

# MCMC setup (shared)
updaters = {
    "population": Tally("pop", alias="population"),
    "cut_edges": cut_edges,
}

initial_partition = Partition(
    graph,
    assignment="SCHOOL_ID",
    updaters=updaters
)

num_districts = len(initial_partition.parts)
ideal_population = sum(initial_partition["population"].values()) / num_districts
pop_tolerance = 0.40

if rank == 0:
    print(f"Initial population per school: {initial_partition['population']}")
    print(f"Ideal population: {ideal_population:.0f}, Tolerance: +/- {pop_tolerance*100}%")

# Define proposal function
def proposal_function(partition):
    return recom(
        partition,
        pop_col="pop",
        pop_target=ideal_population,
        epsilon=pop_tolerance,
        node_repeats=50
    )

# Define constraints
def get_constraints(partition):
    return [within_percent_of_ideal_population(partition, pop_tolerance)]

# Run multiple MCMC chains on this process
for run_id in range(start_run, end_run + 1):
    print(f"Process {rank}: Starting run {run_id}/{TOTAL_RUNS}")
    
    # Set unique seed for this run
    random.seed(42 + run_id)
    
    # Generate new starting partition for this run
    from gerrychain.tree import recursive_tree_part
    new_assignment = recursive_tree_part(
        graph,                               
        parts=range(num_districts),           
        pop_target=ideal_population,          
        pop_col="pop",                        
        epsilon=pop_tolerance               
    )
    
    new_initial_partition = Partition(
        graph,
        assignment=new_assignment,
        updaters=updaters
    )
    
    # Create MarkovChain for this run
    chain = MarkovChain(
        proposal=proposal_function,     
        constraints=get_constraints(new_initial_partition),
        accept=always_accept, 
        initial_state=new_initial_partition, 
        total_steps=1000 
    )
    
    # Run MCMC
    print(f"Process {rank}: Running MCMC for run {run_id}...")
    results = []
    last_partition = new_initial_partition
    
    for i, partition_step in enumerate(chain):
        if (i + 1) % 200 == 0:  # Less frequent printing
            print(f"Process {rank}: Run {run_id}, Step {i + 1}")
        
        district_populations = list(partition_step["population"].values())
        results.append({
            'run': run_id,
            'step': i + 1,
            'cut_edges': len(partition_step['cut_edges']),
            'min_pop': min(district_populations),
            'max_pop': max(district_populations),
            'pop_range': max(district_populations) - min(district_populations),
        })
        last_partition = partition_step
    
    # Process results for this run
    result_gdf = gdf.copy()
    result_gdf['FINAL_DISTRICT'] = result_gdf.index.map(last_partition.assignment)
    result_gdf['FINAL_DISTRICT'] = result_gdf['FINAL_DISTRICT'].astype(str)
    assignment = result_gdf['FINAL_DISTRICT']
    
    # Calculate income distribution
    result_gdf["inc_times_pop"] = result_gdf["indinc"] * result_gdf["pop"]
    result_gdf.reset_index('GEOID', inplace=True)
    districts = (
        result_gdf.dissolve(
            by="FINAL_DISTRICT",
            aggfunc={"inc_times_pop": "sum", "pop": "sum"},
            as_index=False,
        )
        .assign(w_mean_iinc=lambda d: d["inc_times_pop"] / d["pop"])
        .drop(columns=["inc_times_pop"])
    )
    
    # Save plot
    districts.plot(column="w_mean_iinc", cmap="OrRd", legend=True)
    plt.title(f"Weighted Income Distribution - Run {run_id}")
    plt.savefig(f"results/partition_run_{run_id}.png", dpi=300)
    plt.close()
    
    # Save results
    assignment.to_csv(f"output/run_{run_id}.csv", header=["district"], index_label="GEOID")
    
    stats_df = pd.DataFrame(results)
    stats_df.to_csv(f"output/stats_{run_id}.csv", index=False)
    
    print(f"Process {rank}: Completed run {run_id}")

print(f"Process {rank}: All {my_runs} runs completed!")

# Wait for all processes to finish
comm.Barrier()

if rank == 0:
    print("All processes completed!")
    print(f"Generated {TOTAL_RUNS} total runs")
    print("Results are in the output/ directory")
    print("Plots are in the results/ directory")