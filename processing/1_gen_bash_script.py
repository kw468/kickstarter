"""
    Create SH Programs For Processing Kickstarter PIDs
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Tues 23 Sep 2020
-------------------------------------------------------------------------------
notes:
    
--------------------------------------------------------------------------------
contributors:
    Kevin:
        name:       Kevin Williams
        email:      kevin.williams@yale.edu
--------------------------------------------------------------------------------
Copyright 2020 Yale University
"""

lower = list(range(1, 70000, 1000)) # the second number in range needs to be the max
upper = list(range(1000, 70000, 1000))
upper.append(70000)

# outputs a shell script that will schedule 1000 jobs
for i in range(len(upper)):
  L = lower[i]
  U = upper[i]
  script = f'''
  #!/bin/bash
  #BSUB -J "processPIDs[{L}-{U}]"
  #BSUB -oo "../../_out/cleaningPIDs_%I.out"
  #BSUB -n 1
  #BSUB -W 1:00
  #BSUB -m "compute-0-0 compute-0-1 compute-0-2 compute-0-3 compute-0-4 compute-0-5"
  #BSUB -R "rusage[mem=500]"
  
  cd "/gpfs/project/kickstarter/programs/kickstarter/processing"
  source /h4/kw468/anaconda3/etc/profile.d/conda.sh
  conda activate py38
  
  python 2_clean_kickstarter_pid.py ${{LSB_JOBINDEX}}
  '''
  if i < 9:
    f = open("01_0{}_processKickstarterPIDs.sh".format(i+1), "w")
  else:
    f = open("01_{}_processKickstarterPIDs.sh".format(i+1), "w")
  f.write(script)
  f.close()