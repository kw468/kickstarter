#!/bin/bash
for i in $(ls 01_*_processKickstarterPIDs.sh); do bsub < $i;  done