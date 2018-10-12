import os
import time
import argparse
from pprint import pprint

from dask import distributed
from dask_jobqueue import SLURMCluster

def slow_inc(x):
    time.sleep(1)
    return x + 1

def num_crunch(x):
    st = 3.2 * x
    for _ in range(1000000):
        st = st / 1.3
    return st

def main():
    parser = argparse.ArgumentParser(
        description = 'Simple example for using dask-joqueue in SLURM')

    parser.add_argument('--proc_per_job', type = int, default = 1,
                        help = 'Number of processes per job.')
    parser.add_argument('--cores_per_proc', type = float, default = 2,
                        help = 'Number of cores per process.')
    parser.add_argument('--n_jobs', type = int, default = 1,
                        help = 'Number of jobs')
    parser.add_argument('--array', type = int, default = 0,
                        help = 'EXPERIMENTAL. If >0, then submit an job-array '+\
                        'of this size. The total number of jobs will'+\
                        ' be `array * n_jobs`.')
    parser.add_argument('--container', type = str,
                        help = 'Path to singularity container. If `None`, '+\
                        'then assumes conda environment.')
    parser.add_argument('--qos', type = str, help = 'QOS to use.')
    parser.add_argument('--dry', action = 'store_true',
                        help = 'Print job script and exit (no submission)')
    parser.add_argument('--load', type = int, default = 1000,
                        help = 'Load for the function.')
    args = parser.parse_args()

    n_procs = args.proc_per_job * args.n_jobs

    params = {
        'cores' : int(args.cores_per_proc * args.proc_per_job),
        'memory' : '{0:d}00MB'.format(args.proc_per_job*5),
        'processes' : args.proc_per_job,
        # The name to assign to each worker
        'name' : 'dask_test'
    }

    job_extra = ['--requeue']
    env_extra = []

    if not args.qos is None:
        job_extra.append('--qos {}'.format(args.qos))

    if args.array > 0:
        n_procs = n_procs * args.array
        job_extra.append('--array 0-{0:d}'.format(args.array - 1))
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
        env_extra += [ 'source /etc/profile.d/modules.sh',
        'module add openmind/singularity/2.6.0']

    params.update({ 'job_extra' : job_extra,
                    'env_extra' : env_extra})

    cluster = SLURMCluster(**params)
    """
    Display the job script.
    """
    print(cluster.job_script())
    pprint(params)

    t0 = time.time()
    num_crunch(100)
    expected_dur = (time.time() - t0) * args.load
    print('Expected time of linear call: {0:f}'.format(expected_dur))

    if args.dry:
        return

    """
    Scale the cluster to the number of jobs.
    """
    print('Scaling by {}'.format(args.n_jobs))
    cluster.scale_up(args.proc_per_job * args.n_jobs)

    """
    Setup a client that interfaces with the workers
    """
    client = distributed.Client(cluster)
    time.sleep(10)
    print(cluster)
    print(client)
    pprint(client.has_what())
    # pprint(client.scheduler_info())
    """
    Generate a transaction.
    """
    futures = client.map(num_crunch, range(args.load))
    t0 = time.time()

    """
    Compute (and then discard) while keeping track of progress.
    """
    distributed.progress(futures)
    dur = time.time() - t0
    msg = '\n\nSpeed up of {0:f}x ({1:f}/{2:f})'.format((expected_dur / dur),
                                                    expected_dur, dur)
    print(msg)
    msg = 'Ideal speed up is {0:f}x'.format(n_procs)
    print(msg)
    """
    The jobs allocated to `cluster` should end after this file is completed.
    """
    # cluster.close()

if __name__ == '__main__':
    main()

