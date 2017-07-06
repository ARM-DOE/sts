# TODO: automate log/dir checking to confirm:
# 1) files sent in order
# 2) no file left behind
# 3) no duplicates

echo "-------------------------------------------------------------------------"
echo "ERRORS:"
grep "ERROR" $STS_HOME/data/log/messages/*/*
i_sorted=`cut -d ":" -f 1 $STS_HOME/data*/log/incoming_from/*/*/* | sort`
o_sorted=`cut -d ":" -f 1 $STS_HOME/data*/log/outgoing_to/*/*/* | sort`
echo "-------------------------------------------------------------------------"
echo "OUT DUPLICATES:"
echo $o_sorted | uniq -c | grep -v " 1"
echo "-------------------------------------------------------------------------"
echo "IN DUPLICATES:"
echo $i_sorted | uniq -c | grep -v " 1"
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort
