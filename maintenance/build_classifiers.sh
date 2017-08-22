CHASMDIR='/home/rkim/CHASM_production/CHASM/'
contextDir='ClassifierPack/contexts'

export CHASMDIR=$CHASMDIR

cd $CHASMDIR
for contextPath in `ls ClassifierPack/contexts/*.context`; do 
	contextName=`echo $contextPath|cut -f3 -d\/|cut -f1 -d.`; 
	echo $contextName; 
	$CHASMDIR/BuildClassifier -m $CHASMDIR/ClassifierPack/contexts/$contextName.context -i $CHASMDIR/ClassifierPack/drivers.tmps.10lines -o $contextName -f $CHASMDIR/ClassifierPack/features/Features.list
done
