for runfile in ./*/*.run
do 
echo $runfile
#./trec_eval -m $1 trec_pages.qrel $runfile
./eval.sh $runfile $1
done

      
  
  