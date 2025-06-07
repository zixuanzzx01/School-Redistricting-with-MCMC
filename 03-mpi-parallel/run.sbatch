#!/bin/bash

#SBATCH --job-name=find_software
#SBATCH --error=mpi.err
#SBATCH --output=mpi.out
#SBATCH --account=macs30123
#SBATCH --partition=caslake
#SBATCH --ntasks=20
#SBATCH --nodes=1
#SBATCH --time=00:05:00

echo "Job started at $(date)"


module load python/anaconda-2022.05 # Load the anaconda module that has your env
module load mpich/3.2.1             # Load MPI


for batch in {1..5}; do
    echo "Starting batch $batch"
    BATCH_START=$((($batch-1)*20 + 1)) mpirun -np 20 python main.py
    # Move results to batch-specific directory
    mkdir -p batch_${batch}
    mv output/* batch_${batch}/ 2>/dev/null || true
    mv results/* batch_${batch}/ 2>/dev/null || true
done

wait
echo "All batches completed."