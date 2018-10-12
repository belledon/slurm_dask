import time
import argparse

from dask import distributed
from dask_jobqueue import SLURMCluster

def slow_inc(x):
    time.sleep(1)
    return x + 1

def main():
    parser = argparse.ArgumentParse(
        description = 'Simple example for using dask-joqueue in SLUM')

    parser.add_argument('--proc_per_job', type = int, default = 1,
                        help = 'Number of processes per job.')
    parser.add_argument('--n_jobs', type = int, default = 1,
                        help = 'Number of jobs')
    parser.add_argument('--array', type = int, default = 0,
                        help = 'If >0, then submit an job-array of this size'+\
                        '. The total number of jobs will be `array * n_jobs`.')
    parser.add_argument('--container', type = str,
                        help = 'Path to singularity container. If `None`, '+\
                        'then assumes conda environment.')
    parser.add_argument('--qos', type = str, help = 'QOS to use.')
    args = parser.parse_args()

    params = {
        'cores' = int(2 * args.proc_per_job),
        'memory' = '{0:d}00MB'.format(args.proc_per_job*5),
        'processes' = args.proc_per_job,
        # The name to assign to each worker
        'name' = 'dask_test'
    }

    job_extra = ['--requeue']
    env_extra = []

    if not args.qos is None:
        job_extra.append('--qos {}'.format(args.qos))

    if args.array > 0:
        jobs_extra.append('--array 0-{0:d}'.format(args.array - 1))
        """
        This is added to ensure that each worker has a unique ID.
        This may be unnecessary. 
        """
        env_extra.append(
            'JOB_ID=${SLURM_ARRAY_JOB_ID%;*}_${SLURM_ARRAY_TASK_ID%;*}')

    if not args.container is None:
        """
        When using a  container, dask needs to know how to enter the python
        environment.

        Note:
        The binding `-B..` is cluster(OpenMind) specific but can generalize.
        The binding is required since `singularity` will not bind by default.
        """
        cont = os.path.normpath(args.container)
        bind = cont.split(os.sep)[1]
        bind = '-B /{0!s}:/{0!s}'.format(bind)
        py = 'singularity exec {0!s} {1!s} python3'.format(bind, cont)
        params.update({'python' : py})
        """
        Dask will generate a job script but some elements will be missing
        due to the way the singularity container with interface with slurm.
        The `modules` need to initialized and `singularity` needs to be added.
        """
        env_extra.append([ 'source /etc/profile.d/modules.sh',
                           'module add openmind/singularity/2.6.0'])

    params.update({ 'jobs_extra' : jobs_extra,
                    'env_extra' : env_extra})

    cluster = SLURMCluster(**params)
    """
    Display the job script.
    """
    print(cluster.job_script())

    """
    Scale the cluster to the number of jobs.
    """
    cluster.scale(args.n_jobs)

    """
    Setup a client that interfaces with the workers
    """
    client = distributed.Client(cluster)

    """
    Generate a transaction.
    """
    futures = client.map(slow_inc, range(1000))

    """
    Compute (and then discard) while keeping track of progress.
    """
    distributed.progress(futures)

    """
    The jobs allocated to `cluster` should end after this file is completed.
    """
    # cluster.close()

if __name__ = '__main__':
    main()

