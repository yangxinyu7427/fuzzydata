#!/usr/bin/env bash

outdir='../tmp/fuzzydatatest_7/'
nc=20
nr=100
nv=10

python ../fuzzydata/cli.py --wf_client=pandas \
                            --output_dir=$outdir/pandas_example/ \
                            --columns=$nc --rows=$nr --version=$nv \
                            --bfactor=1 \
                            --exclude_ops='["pivot"]'


python ../fuzzydata/cli.py --wf_client=pandas \
                           --replay_dir=$outdir/pandas_example/ \
                           --output_dir=$outdir/pandas/ \
                           --scale_artifact='{"artifact_0": '3000'}'

python ../fuzzydata/cli.py --wf_client=modin \
                           --output_dir=$outdir/modin_dask/ \
                           --replay_dir=$outdir/pandas_example/ \
                           --wf_options='{"modin_engine": "dask"}'\
                           --scale_artifact='{"artifact_0": '3000'}'

python ../fuzzydata/cli.py --wf_client=modin \
                           --output_dir=$outdir/modin_ray/ \
                           --replay_dir=$outdir/pandas_example/ \
                           --wf_options='{"modin_engine": "ray"}'\
                           --scale_artifact='{"artifact_0": '3000'}'

python ../fuzzydata/cli.py --wf_client=sql \
                           --replay_dir=$outdir/pandas_example/ \
                           --output_dir=$outdir/sqlite/ \
                           --scale_artifact='{"artifact_0": '3000'}'