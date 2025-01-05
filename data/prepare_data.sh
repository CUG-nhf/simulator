#!/bin/bash

:<<!
Preparing simulation data.
log_process: process job traces.
vc_dict_generator: generate VC configurations.
!

clusters=('Venus' 'Earth' 'Saturn' 'Uranus')


for cluster in ${clusters[@]}; do
    echo "Parsing ${cluster}"

    # python log_process.py -c=${cluster}
    
    python vc_dict_generator.py -c=${cluster} -d='July'
done

echo 'Done'