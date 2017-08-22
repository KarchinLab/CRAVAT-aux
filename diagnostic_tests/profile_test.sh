pushd ../WebContent/WEB-INF/Wrappers/
JOBID=$1
INPUT=$2
python -m cProfile -o "/home/rkim/profile_$INPUT" ./masteranalyzer.py no /home/rkim/CRAVAT_SANDBOX/WebContent/WEB-INF/Wrappers/../../../CRAVAT_RESULT/$JOBID no $JOBID -e noemail@karchin-web02.icm.jhu.edu -i $JOBID -c Ovary -m /home/rkim/CRAVAT_SANDBOX/WebContent/WEB-INF/Wrappers/../../../CRAVAT_RESULT/$JOBID/$INPUT -u $INPUT -D /home/rkim/CRAVAT_SANDBOX/WebContent/WEB-INF/Wrappers/../../../CRAVAT_RESULT/$JOBID -d "CHASM;SNVGet" -t on -f on -r off -y driver -n 5 -M on -R no
