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
uniq -c <(cat <(echo "$o_sorted")) | grep -v " 1"
echo "-------------------------------------------------------------------------"
echo "IN DUPLICATES:"
uniq -c <(cat <(echo "$i_sorted")) | grep -v " 1"
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f | sort
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data/stage -type f | sort
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort
